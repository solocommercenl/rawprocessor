"""
calculator.py

All financial logic for rawprocessor Stage 1.
Implements VAT-deductible (vated=True) and margin (vated=False) business rules for vehicle imports,
using exact legacy BPM logic and Mongo-driven tables.

FIXED: BPM calculation restored to working state based on legacy calculator.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorDatabase

from config import config
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
        
        # Financial constants from configuration
        self.vat_rate_nl = config.VAT_RATE_NL
        self.vat_rate_de = config.VAT_RATE_DE
        self.bpm_phev_threshold_old = config.BPM_PHEV_THRESHOLD_OLD
        self.bpm_phev_threshold_new = config.BPM_PHEV_THRESHOLD_NEW
        self.default_down_payment_pct = config.DEFAULT_DOWN_PAYMENT_PCT
        self.default_remaining_debt_pct = config.DEFAULT_REMAINING_DEBT_PCT
        
        logger.info(f"Calculator initialized with config: VAT_NL={self.vat_rate_nl}, VAT_DE={self.vat_rate_de}")

    async def calculate_financials(
        self,
        record: dict,
        vated: bool
    ) -> Dict[str, Any]:
        """
        Calculate all financial fields for a vehicle record.
        Uses corrected field mapping to match actual raw data structure.
        Uses configuration values for financial constants.
        """
        try:
            # --- Extract price (primary field) ---
            im_price_org = float(record.get("price", 0))
            if im_price_org <= 0:
                logger.error("Price missing or <= 0 for record: %s", record.get("car_id"))
                raise ValueError("Price missing or zero")

            # --- Constants from site_settings and config ---
            margin_pct = self._get_margin_pct(im_price_org)
            vat_pct = self.vat_rate_nl  # Use configured NL VAT rate
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
                # VAT-deductible: remove German VAT (configured rate) to get net price
                german_vat_multiplier = 1 + self.vat_rate_de
                im_nett_price = round(im_price_org / german_vat_multiplier, 2)
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
            im_bpm_exempt = bpm_data.get("bpm_exempt", False)

            # --- Step 4: Final price calculations ---
            if vated:
                im_price = round(im_total_taxable_price + im_vat_amount + im_bpm_rate, 2)
                im_nett_sales_price = round(im_total_taxable_price + im_bpm_rate, 2)
                im_nett_margin = round(im_total_taxable_price - im_extra_cost_total - im_nett_price, 2)
            else:
                # Margin scheme: add margin to pricing
                im_price = round(im_price_org + im_margin_amount + im_extra_cost_total + im_unforeseen_cost + im_vat_amount + im_bmp_rate, 2)
                im_nett_sales_price = round(im_price_org + im_margin_amount + im_extra_cost_total + im_unforeseen_cost + im_bmp_rate, 2)
                im_nett_margin = round(im_margin_amount + im_unforeseen_cost, 2)

            # --- Step 5: Leasing calculations (using configured percentages) ---
            im_down_payment = round(self.default_down_payment_pct * im_price, 2)
            im_desired_remaining_debt = round(self.default_remaining_debt_pct * im_price, 2)
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
                "im_bpm_exempt": im_bpm_exempt,
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
        FIXED: Restored to working state based on legacy calculator.
        """
        try:
            # Electric vehicles are exempt
            fuel_type_clean = (fuel_type or "").strip().lower()
            if fuel_type_clean in ("electric", "elektrisch"):
                return {"bpm_rate": 0.0, "bpm_exempt": True}

            # Parse registration date
            try:
                if registration_date:
                    if "/" in registration_date:
                        parts = registration_date.split("/")
                        if len(parts) == 2:  # MM/YYYY
                            reg_date = datetime.strptime(registration_date, "%m/%Y")
                        else:  # DD/MM/YYYY
                            reg_date = datetime.strptime(registration_date, "%d/%m/%Y")
                    else:
                        reg_date = datetime.strptime(registration_date, "%Y")
                    
                    registration_month = reg_date.month
                    registration_year = reg_date.year
                else:
                    registration_month = 1
                    registration_year = int(registration_year or 0)
            except (ValueError, TypeError):
                logger.warning("Invalid registration date format: %s", registration_date)
                return {"bpm_rate": 0.0, "bpm_exempt": False}

            if registration_year == 0 or raw_emissions is None:
                logger.warning("Missing registration year or emissions data")
                return {"bpm_rate": 0.0, "bpm_exempt": False}

            # Calculate age-based depreciation
            today = datetime.today()
            age_in_months = (today.year - registration_year) * 12 + (today.month - registration_month)
            if today.month < registration_month and today.year == registration_year:
                age_in_months -= 1
            age_in_months = max(0, age_in_months)
            
            depreciation_pct = await get_depreciation_percentage(self.db, age_in_months)

            # Handle hybrid vehicles with configured thresholds
            is_hybrid_gas = "electric" in fuel_type_clean and "gasoline" in fuel_type_clean
            is_hybrid_diesel = "electric" in fuel_type_clean and "diesel" in fuel_type_clean
            
            if is_hybrid_gas or is_hybrid_diesel:
                # For 2025+ hybrids, use standard tables
                if registration_year >= 2025:
                    fuel_type_lookup = "diesel" if is_hybrid_diesel else "gasoline"
                else:
                    # Pre-2025 hybrids may qualify for PHEV rates using configured thresholds
                    threshold = self.bpm_phev_threshold_old if registration_year < 2020 or (registration_year == 2020 and registration_month < 7) else self.bpm_phev_threshold_new
                    
                    if raw_emissions <= threshold:
                        phev_entry = await get_phev_entry(self.db, registration_year, float(raw_emissions), registration_month)
                        if phev_entry:
                            base_bpm = phev_entry["bpm"]
                            surcharge = (float(raw_emissions) - phev_entry["lower"]) * phev_entry["multiplier"]
                            gross_bpm = base_bpm + surcharge
                            final_bpm = gross_bpm * ((100 - depreciation_pct) / 100)
                            logger.debug(f"PHEV BPM calculation: threshold={threshold}, base={base_bpm}, final={final_bpm}")
                            return {"bpm_rate": round(final_bpm, 2), "bpm_exempt": False}
                    
                    fuel_type_lookup = "diesel" if is_hybrid_diesel else "gasoline"
            else:
                # Map fuel types to lookup keys
                if fuel_type_clean in ("gasoline", "benzine"):
                    fuel_type_lookup = "gasoline"
                elif fuel_type_clean in ("diesel", "dieselkraftstoff"):
                    fuel_type_lookup = "diesel"
                else:
                    fuel_type_lookup = fuel_type_clean

            # Get BPM entry from lookup tables
            bpm_entry = await get_bpm_entry(self.db, registration_year, float(raw_emissions), registration_month, fuel_type_lookup)
            if not bpm_entry:
                logger.warning("No BPM entry found for year=%s, emissions=%s, fuel=%s", 
                             registration_year, raw_emissions, fuel_type_lookup)
                return {"bpm_rate": 0.0, "bpm_exempt": False}

            # Calculate base BPM with surcharge
            base_bpm = bpm_entry["bpm"]
            surcharge = (float(raw_emissions) - bpm_entry["lower"]) * bpm_entry["multiplier"]
            gross_bpm = base_bpm + surcharge

            # Add diesel surcharge if applicable
            if fuel_type_lookup in ("diesel", "electric/diesel") or is_hybrid_diesel:
                diesel_surcharge_data = await get_diesel_surcharge(self.db, registration_year)
                if diesel_surcharge_data and float(raw_emissions) > diesel_surcharge_data["threshold"]:
                    diesel_surcharge = (float(raw_emissions) - diesel_surcharge_data["threshold"]) * diesel_surcharge_data["surcharge_rate"]
                    gross_bpm += diesel_surcharge

            # Apply depreciation
            final_bpm = gross_bpm * ((100 - depreciation_pct) / 100)
            
            logger.debug("BPM calculation: base=%s, surcharge=%s, depreciation=%s%%, final=%s", 
                        base_bpm, surcharge, depreciation_pct, final_bpm)
            
            return {"bpm_rate": round(final_bpm, 2), "bpm_exempt": False}

        except Exception as ex:
            logger.exception("BPM calculation failed: %s", ex)
            return {"bpm_rate": 0.0, "bpm_exempt": False}

    def get_financial_config_summary(self) -> Dict[str, Any]:
        """
        Get summary of financial configuration being used.
        Useful for debugging and validation.
        """
        return {
            "vat_rates": {
                "netherlands": self.vat_rate_nl,
                "germany": self.vat_rate_de
            },
            "bpm_phev_thresholds": {
                "old_period": self.bpm_phev_threshold_old,
                "new_period": self.bpm_phev_threshold_new
            },
            "leasing_defaults": {
                "down_payment_pct": self.default_down_payment_pct,
                "remaining_debt_pct": self.default_remaining_debt_pct
            },
            "site_settings": {
                "licence_plate_fee": self.site_settings.get("licence_plate_fee"),
                "rdw_inspection": self.site_settings.get("rdw_inspection"),
                "transport_cost": self.site_settings.get("transport_cost"),
                "unforeseen_percentage": self.site_settings.get("unforeseen_percentage"),
                "annual_interest_rate": self.site_settings.get("annual_interest_rate")
            }
        }