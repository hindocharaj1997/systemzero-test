"""
Pydantic Schemas for Silver Layer Validation.

Defines typed schemas for each entity. Validation rules are designed to
distinguish between genuinely bad data (quarantine) and legitimate business
edge cases (keep and flag).

Design Decisions:
- Negative transaction quantities are KEPT (returns/refunds)
- Negative product prices are QUARANTINED (pricing error)
- Negative customer total_spend is KEPT (net refund balance)
- Negative invoice quantities are KEPT (credit notes)
- Out-of-range scores (satisfaction > 5) are QUARANTINED
- Negative durations are QUARANTINED (impossible measurement)
"""

from typing import Optional
from datetime import date
from pydantic import BaseModel, Field, field_validator
from dateutil import parser as date_parser


# ──────────────────────────────────────────────────────────────────────────────
# Customer: negative total_spend is legitimate (customer returned more than
# they purchased, net refund balance). We allow it.
# ──────────────────────────────────────────────────────────────────────────────

class CustomerSchema(BaseModel):
    """Customer entity schema."""
    customer_id: str = Field(..., pattern=r"^CUS-\d+$")
    full_name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    gender: Optional[str] = None
    age: Optional[int] = None
    date_of_birth: Optional[str] = None
    address_street: Optional[str] = None
    address_city: Optional[str] = None
    address_state: Optional[str] = None
    address_postal_code: Optional[str] = None
    address_country: Optional[str] = None
    segment: Optional[str] = None
    # Allow negative: customer may have net refund balance
    total_spend: Optional[float] = None
    registration_date: Optional[str] = None
    last_purchase_date: Optional[str] = None
    is_active: Optional[bool] = None
    email_verified: Optional[bool] = None
    phone_verified: Optional[bool] = None
    preferences_newsletter: Optional[bool] = None
    preferences_sms_notifications: Optional[bool] = None
    preferences_preferred_language: Optional[str] = None
    preferences_preferred_currency: Optional[str] = None
    metadata_source: Optional[str] = None
    total_orders: Optional[int] = Field(default=None, ge=0)
    average_order_value: Optional[float] = Field(default=None, ge=0)
    metadata_created_at: Optional[str] = None
    metadata_updated_at: Optional[str] = None
    
    source_file: Optional[str] = Field(default=None, alias="_source_file")
    loaded_at: Optional[str] = Field(default=None, alias="_loaded_at")
    
    model_config = {"extra": "ignore"}
    
    @field_validator("email")
    @classmethod
    def validate_email_format(cls, v):
        """Basic email format validation — nullify invalid emails but keep record."""
        if v is None or v == "":
            return None
        
        # Handle "n/a", "null", "none" commonly found in bad data
        if str(v).lower() in ["n/a", "null", "none", "nan"]:
            return None
            
        import re
        # Simple pattern: has @ with text on both sides, and a dot in the domain
        # Allow apostrophes in name part
        if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", v):
            # Instead of raising ValueError, we set to None to preserve the customer record
            return None
        return v
    
    @field_validator("registration_date", "last_purchase_date")
    @classmethod
    def date_not_in_future(cls, v):
        """Reject dates that are in the future -> set to None (data quality issue), but keep record."""
        if v is None or v == "":
            return v
        try:
            parsed = date_parser.parse(str(v))
            if parsed.date() > date.today():
                # raising ValueError causes the whole record to be quarantined.
                # Instead, we treat the date as invalid (None) but keep the customer.
                return None
        except (ValueError, TypeError) as e:
            if "future" in str(e):
                return None
            # Other parse errors will be caught by downstream validation
            raise
        return v


# ──────────────────────────────────────────────────────────────────────────────
# Product: negative prices ARE quarantined (a product can't cost < $0).
# Missing categories are kept (nullable). Negative ratings quarantined.
# ──────────────────────────────────────────────────────────────────────────────

class ProductSchema(BaseModel):
    """Product entity schema."""
    product_id: str = Field(..., pattern=r"^PRD-\d+$")
    vendor_id: str = Field(..., pattern=r"^VND-\d+$")
    sku: str
    product_name: str
    description: Optional[str] = None
    category: Optional[str] = None       # Missing categories → keep as null
    subcategory: Optional[str] = None
    currency: Optional[str] = None
    price: Optional[float] = Field(default=None, ge=0)  # Negative → quarantine
    cost: Optional[float] = Field(default=None, ge=0)   # Negative → quarantine
    stock_quantity: Optional[int] = None  # Zero stock is valid
    reorder_level: Optional[int] = None
    weight_kg: Optional[float] = None
    rating: Optional[float] = Field(default=None, ge=0, le=5)
    review_count: Optional[int] = None
    is_active: Optional[bool] = None
    tags: Optional[str] = None  # JSON string
    created_date: Optional[str] = None
    last_updated: Optional[str] = None
    
    source_file: Optional[str] = Field(default=None, alias="_source_file")
    loaded_at: Optional[str] = Field(default=None, alias="_loaded_at")
    
    model_config = {"extra": "ignore"}


# ──────────────────────────────────────────────────────────────────────────────
# Transaction: negative quantities are LEGITIMATE (returns/refunds).
# The README explicitly says "Negative quantities (returns/refunds)".
# Zero quantities are also kept (cancelled/placeholder orders).
# Negative total_amount is legitimate (refund credit).
# ──────────────────────────────────────────────────────────────────────────────

class TransactionSchema(BaseModel):
    """Transaction entity schema."""
    transaction_id: str = Field(..., pattern=r"^TXN-[A-F0-9]+$")
    order_id: Optional[str] = None
    customer_id: Optional[str] = None
    product_id: Optional[str] = None
    transaction_date: Optional[str] = None
    transaction_timestamp: Optional[str] = None
    # Allow negative: returns. Allow zero: cancelled orders.
    quantity: Optional[int] = None
    unit_price: Optional[float] = None
    subtotal: Optional[float] = None
    tax_amount: Optional[float] = None
    tax_rate: Optional[float] = None
    shipping_cost: Optional[float] = None
    discount_amount: Optional[float] = None
    discount_percent: Optional[float] = Field(default=None, ge=0, le=100)
    # Allow negative: refund credits
    total_amount: Optional[float] = None
    is_gift: Optional[bool] = None
    notes: Optional[str] = None
    payment_status: Optional[str] = None
    payment_method: Optional[str] = None
    shipping_method: Optional[str] = None
    channel: Optional[str] = None
    region: Optional[str] = None
    order_status: Optional[str] = None
    
    source_file: Optional[str] = Field(default=None, alias="_source_file")
    loaded_at: Optional[str] = Field(default=None, alias="_loaded_at")
    
    model_config = {"extra": "ignore"}


# ──────────────────────────────────────────────────────────────────────────────
# Vendor: inactive status is a legitimate business state, not bad data.
# Pending approval is also valid. Missing contact info is kept (nullable).
# ──────────────────────────────────────────────────────────────────────────────

class VendorSchema(BaseModel):
    """Vendor entity schema."""
    vendor_id: str = Field(..., pattern=r"^VND-\d+$")
    vendor_name: str
    vendor_code: Optional[str] = None
    country: Optional[str] = None
    country_code: Optional[str] = None
    lead_time_days: Optional[int] = None
    payment_terms: Optional[str] = None
    currency: Optional[str] = None
    contact_primary_name: Optional[str] = None
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    address_street: Optional[str] = None
    address_city: Optional[str] = None
    address_state: Optional[str] = None
    address_postal_code: Optional[str] = None
    address_country: Optional[str] = None
    categories: Optional[str] = None  # JSON string
    certifications: Optional[str] = None  # JSON string
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    region: Optional[str] = None
    reliability_score: Optional[float] = Field(default=None, ge=0, le=100)
    # All statuses valid: active, inactive, pending_approval
    status: Optional[str] = None
    
    source_file: Optional[str] = Field(default=None, alias="_source_file")
    loaded_at: Optional[str] = Field(default=None, alias="_loaded_at")
    
    model_config = {"extra": "ignore"}


# ──────────────────────────────────────────────────────────────────────────────
# Invoice: negative quantities are LEGITIMATE (credit notes / returns).
# Payment dates before invoice dates are flagged in Gold, not quarantined
# (could be prepayments). Calculation mismatches are caught in Gold via
# reconciliation_flag.
# ──────────────────────────────────────────────────────────────────────────────

class InvoiceSchema(BaseModel):
    """Invoice entity schema."""
    invoice_id: str = Field(..., pattern=r"^INV-[A-F0-9]+$")
    vendor_id: str
    invoice_number: Optional[str] = None
    invoice_date: Optional[str] = None
    due_date: Optional[str] = None
    payment_date: Optional[str] = None
    po_number: Optional[str] = None
    # Allow negative: credit notes
    subtotal: Optional[float] = None
    tax_rate: Optional[float] = None
    tax_amount: Optional[float] = None
    shipping_handling: Optional[float] = None
    amount_paid: Optional[float] = None
    balance_due: Optional[float] = None
    total_amount: Optional[float] = None
    vendor_name: Optional[str] = None
    currency: Optional[str] = None
    payment_status: Optional[str] = None
    payment_method: Optional[str] = None
    payment_terms: Optional[str] = None  # NET15/30/45/60
    
    @field_validator("payment_method")
    @classmethod
    def validate_payment_method(cls, v):
        """Standardize payment method values."""
        if not v or v.strip() == "":
            return None
        return v.strip().lower().replace(" ", "_")
        
    @field_validator("tax_rate")
    @classmethod
    def validate_tax_rate(cls, v):
        """Tax rate valid range 0.0-1.0."""
        if v is None:
            return None
        if v < 0 or v > 1:
            return None  # Invalid rate -> set to None
        return v
    approved_by: Optional[str] = None
    notes: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    
    source_file: Optional[str] = Field(default=None, alias="_source_file")
    loaded_at: Optional[str] = Field(default=None, alias="_loaded_at")
    
    model_config = {"extra": "ignore"}


class InvoiceLineItemSchema(BaseModel):
    """Parsed invoice line item from line_items_json."""
    invoice_id: str
    line_number: int
    product_id: Optional[str] = None
    description: Optional[str] = None
    quantity: Optional[int] = None  # Allow negative: credit lines
    unit_cost: Optional[float] = None
    line_total: Optional[float] = None
    
    source_file: Optional[str] = Field(default=None, alias="_source_file")
    loaded_at: Optional[str] = Field(default=None, alias="_loaded_at")
    
    model_config = {"extra": "ignore"}


# ──────────────────────────────────────────────────────────────────────────────
# Review: empty review_ids are quarantined (required field).
# Negative/out-of-range ratings are quarantined (data entry error).
# ──────────────────────────────────────────────────────────────────────────────

class ReviewSchema(BaseModel):
    """Review entity schema."""
    review_id: str
    product_id: str
    customer_id: str
    rating: Optional[int] = Field(default=None, ge=1, le=5)  # 1-5 only
    title: Optional[str] = None
    review_text: Optional[str] = None
    review_date: Optional[str] = None
    sentiment: Optional[str] = None
    verified_purchase: Optional[bool] = None
    helpful_votes: Optional[int] = None
    images: Optional[str] = None  # JSON string
    response: Optional[str] = None
    response_responder: Optional[str] = None
    response_response_text: Optional[str] = None
    response_response_date: Optional[str] = None
    
    source_file: Optional[str] = Field(default=None, alias="_source_file")
    loaded_at: Optional[str] = Field(default=None, alias="_loaded_at")
    
    model_config = {"extra": "ignore"}
    
    @field_validator("review_id")
    @classmethod
    def review_id_not_empty(cls, v):
        if not v or v.strip() == "":
            raise ValueError("review_id cannot be empty")
        return v


# ──────────────────────────────────────────────────────────────────────────────
# Support Ticket: empty ticket_ids are quarantined.
# Satisfaction scores > 5 are quarantined (out of range).
# Inconsistent channel/status case is cleaned in Silver cleaning step.
# ──────────────────────────────────────────────────────────────────────────────

class SupportTicketSchema(BaseModel):
    """Support ticket entity schema."""
    ticket_id: str
    customer_id: str
    product_id: Optional[str] = None
    channel: Optional[str] = None
    priority: Optional[str] = None
    status: Optional[str] = None
    satisfaction_score: Optional[int] = Field(default=None, ge=1, le=5)
    created_at: Optional[str] = None
    resolved_at: Optional[str] = None
    agent_id: Optional[str] = None
    transcript: Optional[str] = None
    summary: Optional[str] = None
    tags: Optional[str] = None  # JSON string
    resolution_type: Optional[str] = None
    
    source_file: Optional[str] = Field(default=None, alias="_source_file")
    loaded_at: Optional[str] = Field(default=None, alias="_loaded_at")
    
    model_config = {"extra": "ignore"}
    
    @field_validator("ticket_id")
    @classmethod
    def ticket_id_not_empty(cls, v):
        if not v or v.strip() == "":
            raise ValueError("ticket_id cannot be empty")
        return v


# ──────────────────────────────────────────────────────────────────────────────
# Call Transcript: negative durations are quarantined (impossible).
# Missing ticket references are kept (not all calls are tied to tickets).
# Quality scores outside 0-100 range are quarantined.
# ──────────────────────────────────────────────────────────────────────────────

class CallTranscriptSchema(BaseModel):
    """Call transcript entity schema."""
    call_id: str
    ticket_id: Optional[str] = None       # Not all calls have tickets
    customer_id: str
    agent_id: Optional[str] = None
    agent_name: Optional[str] = None
    call_start: Optional[str] = None
    call_end: Optional[str] = None
    duration_seconds: Optional[int] = Field(default=None, ge=0)  # Negative → quarantine
    call_type: Optional[str] = None
    phone_number: Optional[str] = None
    queue_wait_seconds: Optional[int] = None
    hold_time_seconds: Optional[int] = None
    transfers: Optional[int] = Field(default=None, ge=0)
    language: Optional[str] = None
    quality_score: Optional[float] = Field(default=None, ge=0, le=100)
    resolution_achieved: Optional[bool] = None
    sentiment_overall: Optional[str] = None
    utterances: Optional[str] = None  # JSON string
    keywords_detected: Optional[str] = None  # JSON string
    action_items: Optional[str] = None  # JSON string
    
    source_file: Optional[str] = Field(default=None, alias="_source_file")
    loaded_at: Optional[str] = Field(default=None, alias="_loaded_at")
    
    model_config = {"extra": "ignore"}


# Schema registry
SCHEMA_REGISTRY = {
    "customer": CustomerSchema,
    "product": ProductSchema,
    "transaction": TransactionSchema,
    "vendor": VendorSchema,
    "invoice": InvoiceSchema,
    "invoice_line_item": InvoiceLineItemSchema,
    "review": ReviewSchema,
    "support_ticket": SupportTicketSchema,
    "call_transcript": CallTranscriptSchema,
}


def get_pydantic_schema(schema_name: str) -> type[BaseModel]:
    """Get Pydantic schema class by name."""
    schema = SCHEMA_REGISTRY.get(schema_name)
    if not schema:
        raise ValueError(f"Unknown schema: {schema_name}. Available: {list(SCHEMA_REGISTRY.keys())}")
    return schema
