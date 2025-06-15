"""
calculator.py

All financial logic for rawprocessor Stage 1.
Implements VAT-deductible (vated=True) and margin (vated=False) business rules for vehicle imports,
using exact legacy BPM logic and Mongo-driven tables.

Author: Autodex Backend
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorDatabase

from utils import (
    parse_registration_date,
    calculate_age_in_months,
    get_depreciation_percentage,
    get_bpm_entry,
    get_phev_entry,
    get_diesel_surcharge
)

logger = logging.getLogger("rawprocessor.calculator")

class Calculator:
    def __init__(self, db: AsyncIOMotorDatabase, site_settings: dict):
        self.db = db
        self.site_settings = site_settings

    async def calculate_financials(
        self,
        record: dict,
        vated: bool
    ) -> Dict[str, Any]:
        try:
            # --- Constants from site_settings ---
            price_for_margin = float(record.get("im_price_org") or record.get("price") or 0)
            margin_pct = self._get_margin_pct(price_for_margin)
            vat_pct = 0.21
            licence_plate_fee = float(self.site_settings.get("licence_plate_fee", 125))
            rdw_inspection = float(self.site_settings.get("rdw_inspection", 300))
            transport_cost = float(self.site_settings.get("transport_cost", 750))
            unforeseen_pct = float(self.site_settings.get("unforeseen_percentage", 0.017))
            interest_rate = float(self.site_settings.get("annual_interest_rate", 0.08))
            loan_term_months = int(self.site_settings.get("loan_term_months", 72))

            # --- Raw price fallback ---
            im_price_org = float(record.get("im_price_org") or record.get("price") or 0)
            if im_price_org <= 0:
                logger.error("im_price_org (or price) missing or <= 0 for record: %s", record)
                raise ValueError("im_price_org missing or zero")

            registration_date = (
                record.get("im_first_registration")
                or record.get("registration")
                or record.get("vehiclehistory", {}).get("Firstregistration")
            )
            registration_year = (
                int(record.get("im_registration_year", 0) or record.get("registration_year", 0) or 0)
            )
            fuel_type = (
                record.get("im_fuel_type")
                or record.get("Fueltype")
                or record.get("energyconsumption", {}).get("Fueltype", "")
            ).lower()
            raw_emissions = (
                record.get("im_raw_emissions")
                or record.get("raw_emissions")
                or record.get("energyconsumption", {}).get("raw_emissions")
            )
            if raw_emissions is not None:
                try:
                    raw_emissions = float(raw_emissions)
                except Exception:
                    raw_emissions = None

            # --- Step 1: Base price calculations ---
            if vated:
                im_nett_price = round(im_price_org / 1.19, 2)
            else:
                im_nett_price = im_price_org

            im_margin_amount = round(im_nett_price * margin_pct, 2) if vated else round(im_price_org * margin_pct, 2)
            im_extra_cost_total = round(licence_plate_fee + transport_cost + rdw_inspection, 2)
            im_unforeseen_cost = round(im_nett_price * unforeseen_pct, 2) if vated else round(im_price_org * unforeseen_pct, 2)

            # --- Step 2: Taxable/Total calculations ---
            if vated:
                im_total_taxable_price = im_nett_price + im_margin_amount + im_extra_cost_total + im_unforeseen_cost
                im_vat_amount = round(im_total_taxable_price * vat_pct, 2)
            else:
                im_taxable_part = im_margin_amount + im_extra_cost_total + im_unforeseen_cost
                im_vat_amount = round(im_taxable_part * vat_pct, 2)

            # --- Step 3: BPM Calculation ---
            bpm_data = await self.calculate_bpm(
                raw_emissions=raw_emissions,
                fuel_type=fuel_type,
                registration_date=registration_date,
                registration_year=registration_year
            )
            im_bpm_rate = round(bpm_data.get("bpm_rate", 0.0), 2)
            im_bpm_exempt = bpm_data.get("bpm_exempt", False)

            # --- Step 4: Final price ---
            if vated:
                im_price = round(im_total_taxable_price + im_vat_amount + im_bpm_rate, 2)
                im_nett_sales_price = round(im_total_taxable_price + im_bpm_rate, 2)
                im_nett_margin = round(im_total_taxable_price - im_extra_cost_total - im_nett_price, 2)
            else:
                im_price = round(im_price_org + im_extra_cost_total + im_unforeseen_cost + im_vat_amount + im_bpm_rate, 2)
                im_nett_sales_price = round(im_price_org + im_extra_cost_total + im_unforeseen_cost + im_bpm_rate, 2)
                im_nett_margin = round(im_nett_sales_price - im_extra_cost_total - im_bpm_rate - im_price_org, 2)

            # --- Step 5: Down payment, remaining debt, monthly payment ---
            im_down_payment = round(0.10 * im_price, 2)
            im_desired_remaining_debt = round(0.20 * im_price, 2)
            im_monthly_payment = self._calculate_monthly_payment(
                price=im_price,
                down_payment=im_down_payment,
                remaining_debt=im_desired_remaining_debt,
                annual_interest=interest_rate,
                term_months=loan_term_months
            )

            result = {
                "im_nett_price": im_nett_price,
                "im_margin_amount": im_margin_amount,
                "im_extra_cost_total": im_extra_cost_total,
                "im_unforeseen_cost": im_unforeseen_cost,
                "im_vat_amount": im_vat_amount,
                "im_bpm_rate": im_bpm_rate,
                "im_bpm_exempt": im_bpm_exempt,
                "im_price": im_price,
                "im_nett_sales_price": im_nett_sales_price,
                "im_nett_margin": im_nett_margin,
                "im_down_payment": im_down_payment,
                "im_desired_remaining_debt": im_desired_remaining_debt,
                "im_monthly_payment": im_monthly_payment
            }
            logger.info("Calculated financials for record (ad_id=%s): %s", record.get("im_ad_id") or record.get("ad_id") or record.get("_id"), result)
            return result

        except Exception as ex:
            logger.exception("Financial calculation error: %s", ex)
            raise

    def _get_margin_pct(self, price_org: float) -> float:
        try:
            for band in self.site_settings.get("price_margins", []):
                if band["max"] is None or price_org <= band["max"]:
                    return float(band["margin"])
            return 0.08
        except Exception as ex:
            logger.error("Error in margin lookup: %s", ex)
            return 0.08

    @staticmethod
    def _calculate_monthly_payment(
        price: float, down_payment: float, remaining_debt: float, annual_interest: float, term_months: int
    ) -> float:
        try:
            principal = price - down_payment - remaining_debt
            if principal <= 0 or term_months <= 0:
                return 0.0
            monthly_interest = annual_interest / 12
            payment = (principal * monthly_interest) / (1 - (1 + monthly_interest) ** -term_months)
            return round(payment, 2)
        except Exception as ex:
            logger.error("Monthly payment calculation failed: %s", ex)
            return 0.0

    async def calculate_bpm(
        self,
        raw_emissions: Optional[float],
        fuel_type: str,
        registration_date: Optional[str],
        registration_year: Optional[int]
    ) -> Dict[str, Any]:
        try:
            if fuel_type in ("electric", "elektrisch"):
                return {"bpm_rate": 0.0, "bpm_exempt": True}

            reg_month, reg_year = parse_registration_date(registration_date, registration_year)
            if reg_year == 0:
                return {"bpm_rate": 0.0, "bpm_exempt": False}

            today = datetime.today()
            age_months = calculate_age_in_months(today, reg_month, reg_year)
            depreciation_pct = await get_depreciation_percentage(self.db, age_months)

            is_hybrid_gas = fuel_type in ("hybride-benzine", "electric/gasoline")
            is_hybrid_diesel = fuel_type in ("hybride-diesel", "electric/diesel")
            if is_hybrid_gas or is_hybrid_diesel:
                if reg_year >= 2025:
                    fuel_type_lookup = "diesel" if is_hybrid_diesel else "benzine"
                else:
                    threshold = 50 if reg_year < 2020 or (reg_year == 2020 and reg_month < 7) else 60
                    if raw_emissions is not None and float(raw_emissions) <= threshold:
                        phev_entry = await get_phev_entry(self.db, reg_year, float(raw_emissions), reg_month)
                        if phev_entry:
                            base_bpm = phev_entry["bpm"]
                            surcharge = (float(raw_emissions) - phev_entry["lower"]) * phev_entry["multiplier"]
                            gross_bpm = base_bpm + surcharge
                            final_bpm = gross_bpm * ((100 - depreciation_pct) / 100)
                            return {"bpm_rate": round(final_bpm, 2), "bpm_exempt": False}
                    fuel_type_lookup = "diesel" if is_hybrid_diesel else "benzine"
            else:
                fuel_type_lookup = fuel_type

            bpm_entry = await get_bpm_entry(self.db, reg_year, float(raw_emissions or 0), reg_month, fuel_type_lookup)
            if not bpm_entry:
                return {"bpm_rate": 0.0, "bpm_exempt": False}
            base_bpm = bpm_entry["bpm"]
            surcharge = (float(raw_emissions or 0) - bpm_entry["lower"]) * bpm_entry["multiplier"]
            gross_bpm = base_bpm + surcharge

            if fuel_type_lookup in ("diesel", "hybride-diesel", "electric/diesel"):
                diesel = await get_diesel_surcharge(self.db, reg_year)
                if diesel and float(raw_emissions or 0) > diesel["threshold"]:
                    gross_bpm += (float(raw_emissions) - diesel["threshold"]) * diesel["surcharge_rate"]

            final_bpm = gross_bpm * ((100 - depreciation_pct) / 100)
            return {"bpm_rate": round(final_bpm, 2), "bpm_exempt": False}

        except Exception as ex:
            logger.exception("BPM calculation failed: %s", ex)
            return {"bpm_rate": 0.0, "bpm_exempt": False}
