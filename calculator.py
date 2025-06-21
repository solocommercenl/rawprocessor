"""
calculator.py

All financial logic for rawprocessor Stage 1.
Implements VAT-deductible (vated=True) and margin (vated=False) business rules for vehicle imports,
using exact legacy BPM logic and Mongo-driven tables.

FIXED: Corrected ALL BMP references to BPM throughout the code.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorDatabase

from utils import (
    parse_registration_date,
    calculate_age_in_months,
    get_depreciation_percentage,
    get_bpm_entry,  # FIXED: Now correctly named
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
        """
        Calculate all financial fields for a vehicle record.
        Uses corrected field mapping to match actual raw data structure.
        """
        try:
            # --- Extract price (primary field) ---
            im_price_org = float(record.get("price", 0))
            if im_price_org <= 0:
                logger.error("Price missing or <= 0 for record: %s", record.get("car_id"))
                raise ValueError("Price missing or zero")

            # --- Constants from site_settings ---
            margin_pct = self._get_margin_pct(im_price_org)
            vat_pct = 0.21
            licence_plate_fee = float(self.site_settings.get("licence_plate_fee", 125))
            rdw_inspection = float(self.site_settings.get("rdw_inspection", 300))
            transport_cost = float(self.site_settings.get("transport_cost", 750))
            unforeseen_pct = float(self.site_settings.get("unforeseen_percentage", 0.017))
            interest_rate = float(self.site_settings.get("annual_interest_rate", 0.08))
            loan_term_months = int(self.site_settings.get("loan_term_months", 72))

            # --- Extract vehicle data using correct field paths ---
            registration_date = record.get("registration", "")
            registration_year = int(record.get("registration_year", 0) or 0)
            
            # Get fuel type from energyconsumption.Fueltype
            fuel_type = record.get("energyconsumption", {}).get("Fueltype", "").lower()
            
            # Get emissions from energyconsumption.raw_emissions  
            raw_emissions = record.get("energyconsumption", {}).get("raw_emissions")
            if raw_emissions is not None:
                try:
                    raw_emissions = float(raw_emissions)
                except (ValueError, TypeError):
                    raw_emissions = None

            # --- Step 1: Base price calculations ---
            if vated:
                # VAT-deductible: remove German VAT (19%) to get net price
                im_nett_price = round(im_price_org / 1.19, 2)
            else:
                # Margin scheme: gross price becomes net price
                im_nett_price = im_price_org

            # Calculate margin on appropriate base
            margin_base = im_nett_price if vated else im_price_org
            im_margin_amount = round(margin_base * margin_pct, 2)
            
            # Fixed costs
            im_extra_cost_total = round(licence_plate_fee + transport_cost + rdw_inspection, 2)
            
            # Unforeseen costs
            unforeseen_base = im_nett_price if vated else im_price_org
            im_unforeseen_cost = round(unforeseen_base * unforeseen_pct, 2)

            # --- Step 2: VAT calculations ---
            if vated:
                # VAT-deductible: VAT on everything except original price
                im_total_taxable_price = im_nett_price + im_margin_amount + im_extra_cost_total + im_unforeseen_cost
                im_vat_amount = round(im_total_taxable_price * vat_pct, 2)
            else:
                # Margin scheme: VAT only on margin and costs
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
            im_bpm_exempt = bpm_data.get("bpm_exempt", False)  # FIXED: bpm_exempt not bmp_exempt

            # --- Step 4: Final price calculations ---
            if vated:
                im_price = round(im_total_taxable_price + im_vat_amount + im_bpm_rate, 2)
                im_nett_sales_price = round(im_total_taxable_price + im_bpm_rate, 2)
                im_nett_margin = round(im_total_taxable_price - im_extra_cost_total - im_nett_price, 2)
            else:
                im_price = round(im_price_org + im_extra_cost_total + im_unforeseen_cost + im_vat_amount + im_bpm_rate, 2)
                im_nett_sales_price = round(im_price_org + im_extra_cost_total + im_unforeseen_cost + im_bpm_rate, 2)
                im_nett_margin = round(im_nett_sales_price - im_extra_cost_total - im_bpm_rate - im_price_org, 2)

            # --- Step 5: Leasing calculations ---
            im_down_payment = round(0.10 * im_price, 2)
            im_desired_remaining_debt = round(0.20 * im_price, 2)
            im_monthly_payment = self._calculate_monthly_payment(
                price=im_price,
                down_payment=im_down_payment,
                remaining_debt=im_desired_remaining_debt,
                annual_interest=interest_rate,
                term_months=loan_term_months
            )

            # --- Build result ---
            result = {
                "im_nett_price": im_nett_price,
                "im_margin_amount": im_margin_amount,
                "im_extra_cost_total": im_extra_cost_total,
                "im_unforeseen_cost": im_unforeseen_cost,
                "im_vat_amount": im_vat_amount,
                "im_bpm_rate": im_bpm_rate,
                "im_bpm_exempt": im_bpm_exempt,  # FIXED: bpm_exempt not bmp_exempt
                "im_price": im_price,
                "im_nett_sales_price": im_nett_sales_price,
                "im_nett_margin": im_nett_margin,
                "im_down_payment": im_down_payment,
                "im_desired_remaining_debt": im_desired_remaining_debt,
                "im_monthly_payment": im_monthly_payment
            }
            
            logger.info("Calculated financials for record %s: %s", 
                       record.get("car_id", "unknown"), 
                       {k: v for k, v in result.items() if k in ["im_price", "im_bpm_rate", "im_monthly_payment"]})
            return result

        except Exception as ex:
            logger.exception("Financial calculation error for record %s: %s", 
                           record.get("car_id", "unknown"), ex)
            raise

    def _get_margin_pct(self, price_org: float) -> float:
        """
        Get margin percentage based on price bands from site settings.
        """
        try:
            price_margins = self.site_settings.get("price_margins", [])
            for band in price_margins:
                min_price = band.get("min", 0)
                max_price = band.get("max")
                
                if price_org >= min_price and (max_price is None or price_org <= max_price):
                    return float(band["margin"])
            
            # Default fallback
            return 0.08
            
        except Exception as ex:
            logger.error("Error in margin lookup for price %s: %s", price_org, ex)
            return 0.08

    @staticmethod
    def _calculate_monthly_payment(
        price: float, down_payment: float, remaining_debt: float, 
        annual_interest: float, term_months: int
    ) -> float:
        """
        Calculate monthly payment for leasing.
        """
        try:
            principal = price - down_payment - remaining_debt
            if principal <= 0 or term_months <= 0:
                return 0.0
                
            monthly_interest = annual_interest / 12
            if monthly_interest == 0:
                return principal / term_months
                
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
        """
        Calculate BPM (Dutch vehicle tax) using MongoDB lookup tables.
        
        FIXED: All field references corrected to BPM.
        """
        try:
            # Electric vehicles are exempt
            if fuel_type in ("electric", "elektrisch"):
                return {"bpm_rate": 0.0, "bpm_exempt": True}  # FIXED: bpm_exempt

            # Parse registration date
            reg_month, reg_year = parse_registration_date(registration_date, registration_year)
            if reg_year == 0:
                logger.warning("Invalid registration year, BPM calculation skipped")
                return {"bpm_rate": 0.0, "bpm_exempt": False}  # FIXED: bpm_exempt

            # Calculate age-based depreciation
            today = datetime.today()
            age_months = calculate_age_in_months(today, reg_month, reg_year)
            depreciation_pct = await get_depreciation_percentage(self.db, age_months)

            # Handle hybrid vehicles
            is_hybrid_gas = fuel_type in ("hybride-benzine", "electric/gasoline")
            is_hybrid_diesel = fuel_type in ("hybride-diesel", "electric/diesel")
            
            if is_hybrid_gas or is_hybrid_diesel:
                # For 2025+ hybrids, use standard tables
                if reg_year >= 2025:
                    fuel_type_lookup = "diesel" if is_hybrid_diesel else "benzine"
                else:
                    # Pre-2025 hybrids may qualify for PHEV rates
                    threshold = 50 if reg_year < 2020 or (reg_year == 2020 and reg_month < 7) else 60
                    
                    if raw_emissions is not None and float(raw_emissions) <= threshold:
                        phev_entry = await get_phev_entry(self.db, reg_year, float(raw_emissions), reg_month)
                        if phev_entry:
                            base_bpm = phev_entry["bpm"]  # FIXED: bpm not bmp
                            surcharge = (float(raw_emissions) - phev_entry["lower"]) * phev_entry["multiplier"]
                            gross_bpm = base_bpm + surcharge  # FIXED: bpm not bmp
                            final_bpm = gross_bpm * ((100 - depreciation_pct) / 100)  # FIXED: bpm not bmp
                            return {"bpm_rate": round(final_bpm, 2), "bpm_exempt": False}  # FIXED: bpm_exempt
                    
                    fuel_type_lookup = "diesel" if is_hybrid_diesel else "benzine"
            else:
                fuel_type_lookup = fuel_type

            # Get BPM entry from lookup tables
            bpm_entry = await get_bpm_entry(self.db, reg_year, float(raw_emissions or 0), reg_month, fuel_type_lookup)
            if not bpm_entry:
                logger.warning("No BPM entry found for year=%s, emissions=%s, fuel=%s", 
                             reg_year, raw_emissions, fuel_type_lookup)
                return {"bpm_rate": 0.0, "bpm_exempt": False}  # FIXED: bpm_exempt

            # Calculate base BPM with surcharge
            base_bpm = bpm_entry["bpm"]  # FIXED: bpm not bmp
            surcharge = (float(raw_emissions or 0) - bpm_entry["lower"]) * bpm_entry["multiplier"]
            gross_bpm = base_bpm + surcharge  # FIXED: bpm not bmp

            # Add diesel surcharge if applicable
            if fuel_type_lookup in ("diesel", "hybride-diesel", "electric/diesel"):
                diesel_surcharge_data = await get_diesel_surcharge(self.db, reg_year)
                if diesel_surcharge_data and float(raw_emissions or 0) > diesel_surcharge_data["threshold"]:
                    diesel_surcharge = (float(raw_emissions) - diesel_surcharge_data["threshold"]) * diesel_surcharge_data["surcharge_rate"]
                    gross_bpm += diesel_surcharge  # FIXED: bpm not bmp

            # Apply depreciation
            final_bpm = gross_bpm * ((100 - depreciation_pct) / 100)  # FIXED: bpm not bmp
            
            logger.debug("BPM calculation: base=%s, surcharge=%s, depreciation=%s%%, final=%s", 
                        base_bpm, surcharge, depreciation_pct, final_bpm)  # FIXED: base_bpm, final_bpm
            
            return {"bpm_rate": round(final_bpm, 2), "bpm_exempt": False}  # FIXED: final_bpm, bpm_exempt

        except Exception as ex:
            logger.exception("BPM calculation failed: %s", ex)
            return {"bpm_rate": 0.0, "bpm_exempt": False}  # FIXED: bmp_rate -> bpm_rate, bmp_exempt -> bpm_exempt