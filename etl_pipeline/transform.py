import pandas as pd
import re
import hashlib
import logging
from typing import Dict, List, Optional
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def clean_string(s: str) -> str:
    if pd.isna(s):
        return ""
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def clean_data_source1(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning Source 1 data...")
    
    df = df.copy()
    
    if 'activity_places' in df.columns:
        if df['activity_places'].dtype == 'object':
            sample = df['activity_places'].dropna().iloc[0] if len(df['activity_places'].dropna()) > 0 else None
            if isinstance(sample, list):
                def clean_activity_list(x):
                    if isinstance(x, list):
                        return [item.strip() if isinstance(item, str) else str(item) for item in x]
                    return []
                df['activity_places_clean'] = df['activity_places'].apply(clean_activity_list)
            else:
                def parse_activity_string(x):
                    if x is None or (isinstance(x, float) and pd.isna(x)):
                        return []
                    return [item.strip() for item in str(x).split(';') if item.strip()]
                df['activity_places_clean'] = df['activity_places'].apply(parse_activity_string)
        else:
            df['activity_places_clean'] = df['activity_places'].apply(lambda x: [] if pd.isna(x) else [str(x)])
    
    if 'top_suppliers' in df.columns:
        if df['top_suppliers'].dtype == 'object':
            sample = df['top_suppliers'].dropna().iloc[0] if len(df['top_suppliers'].dropna()) > 0 else None
            if isinstance(sample, list):
                def clean_suppliers_list(x):
                    if isinstance(x, list):
                        return [item.strip() if isinstance(item, str) else str(item) for item in x]
                    return []
                df['top_suppliers_clean'] = df['top_suppliers'].apply(clean_suppliers_list)
            else:
                def parse_suppliers_string(x):
                    if x is None or (isinstance(x, float) and pd.isna(x)):
                        return []
                    return [item.strip() for item in str(x).split(';') if item.strip()]
                df['top_suppliers_clean'] = df['top_suppliers'].apply(parse_suppliers_string)
        else:
            df['top_suppliers_clean'] = df['top_suppliers'].apply(lambda x: [] if pd.isna(x) else [str(x)])
    
    df['corporate_name_clean'] = df['corporate_name_S1'].apply(
        lambda x: str(x).strip() if pd.notna(x) else ''
    )
    
    df['address_clean'] = df['address'].apply(
        lambda x: ' '.join(str(x).split()) if pd.notna(x) else ''
    )
    
    logger.info(f"Source 1 cleaned: {len(df)} records")
    return df


def clean_data_source2(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning Source 2 data...")
    
    df = df.copy()
    
    if 'main_customers' in df.columns:
        if df['main_customers'].dtype == 'object':
            sample = df['main_customers'].dropna().iloc[0] if len(df['main_customers'].dropna()) > 0 else None
            if isinstance(sample, list):
                def clean_customers_list(x):
                    if isinstance(x, list):
                        cleaned_items: List[str] = []
                        for item in x:
                            if item is None or (isinstance(item, float) and pd.isna(item)):
                                continue
                            s = str(item).strip()
                            if not s:
                                continue
                            if s.startswith('[') and s.endswith(']') and ("'" in s or '"' in s):
                                inner = s[1:-1].strip()
                                tokens = re.findall(r"'([^']+)'", inner) or re.findall(r"\"([^\"]+)\"", inner)
                                cleaned_items.extend(t.strip() for t in tokens if t.strip())
                            else:
                                cleaned_items.append(s)
                        return cleaned_items
                    return []
                df['main_customers_clean'] = df['main_customers'].apply(clean_customers_list)
            else:
                def parse_customers_string(x):
                    if x is None or (isinstance(x, float) and pd.isna(x)):
                        return []
                    s = str(x).strip()
                    if not s:
                        return []
                    if s.startswith('[') and s.endswith(']') and ("'" in s or '"' in s):
                        inner = s[1:-1].strip()
                        tokens = re.findall(r"'([^']+)'", inner) or re.findall(r"\"([^\"]+)\"", inner)
                        return [t.strip() for t in tokens if t.strip()]
                    s = s.replace("\n", " ")
                    tokens = re.split(r"[;,|]", s)
                    if len(tokens) == 1:
                        tokens = re.split(r"\s{2,}", s)
                    if len(tokens) == 1:
                        tokens = s.split(" ")
                    return [t.strip() for t in tokens if t.strip()]
                df['main_customers_clean'] = df['main_customers'].apply(parse_customers_string)
        else:
            df['main_customers_clean'] = df['main_customers'].apply(
                lambda x: [] if pd.isna(x) else [str(x).strip()]
            )
    
    df['corporate_name_clean'] = df['corporate_name_S2'].apply(
        lambda x: str(x).strip() if pd.notna(x) else ''
    )
    
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0.0)
    df['profit'] = pd.to_numeric(df['profit'], errors='coerce').fillna(0.0)
    
    def compute_profit_margin(row):
        revenue = row['revenue']
        profit = row['profit']
        if pd.isna(revenue) or revenue <= 0 or pd.isna(profit):
            return 0.0
        return (profit / revenue) * 100

    df['profit_margin'] = df.apply(compute_profit_margin, axis=1)
    
    def categorize_revenue(revenue):
        if pd.isna(revenue):
            return 'Unknown'
        elif revenue < 100:
            return 'Small'
        elif revenue < 1000:
            return 'Medium'
        elif revenue < 5000:
            return 'Large'
        else:
            return 'Enterprise'
    
    df['revenue_category'] = df['revenue'].apply(categorize_revenue)
    
    logger.info(f"Source 2 cleaned: {len(df)} records")
    return df


def resolve_entities(df_s1: pd.DataFrame, df_s2: pd.DataFrame) -> pd.DataFrame:
    logger.info("Resolving entities...")
    
    df_s1 = df_s1.copy()
    df_s2 = df_s2.copy()
    
    df_s1["name_key"] = df_s1["corporate_name_clean"].apply(clean_string)
    df_s1["address_key"] = df_s1["address_clean"].apply(clean_string)
    
    df_s2["name_key"] = df_s2["corporate_name_clean"].apply(clean_string)
    
    df_merged = pd.merge(
        df_s1,
        df_s2,
        on="name_key",
        how="inner",
        suffixes=("_s1", "_s2")
    )
    
    def make_corporate_id(row):
        name = row.get('name_key', '')
        address = row.get('address_key', '')
        base = f"{name}_{address}"
        return hashlib.sha256(base.encode()).hexdigest()[:16]
    
    df_merged["corporate_id"] = df_merged.apply(make_corporate_id, axis=1)
    
    logger.info(f"Entities resolved: {len(df_merged)} unique entities")
    return df_merged


def harmonize_schema(df_merged: pd.DataFrame) -> pd.DataFrame:
    logger.info("Harmonizing schema...")
    
    df = df_merged.copy()

    s1_present = df.get("corporate_name_S1", pd.Series([pd.NA] * len(df))).notna()
    s2_present = df.get("corporate_name_S2", pd.Series([pd.NA] * len(df))).notna()
    matched = (s1_present & s2_present).sum()
    s1_only = (s1_present & ~s2_present).sum()
    s2_only = (~s1_present & s2_present).sum()
    neither = (~s1_present & ~s2_present).sum()
    
    def safe_get(col, default=None):
        if col in df.columns:
            return df[col]
        return pd.Series([default] * len(df))
    
    corporate_registry_df = pd.DataFrame({
        "corporate_id": df["corporate_id"],
        "corporate_name": safe_get("corporate_name_S1", "").fillna(
            safe_get("corporate_name_S2", "")
        ),
        "address": safe_get("address", "").fillna(""),
        "activity_places": safe_get("activity_places_clean", []),
        "top_suppliers": safe_get("top_suppliers_clean", []),
        "main_customers": safe_get("main_customers_clean", []),
        "revenue": safe_get("revenue", 0.0).fillna(0.0),
        "profit": safe_get("profit", 0.0).fillna(0.0),
        "profit_margin": safe_get("profit_margin", 0.0).fillna(0.0),
        "revenue_category": safe_get("revenue_category", "Unknown").fillna("Unknown"),
    })

    for col in ["corporate_name", "address", "revenue_category"]:
        corporate_registry_df[col] = (
            corporate_registry_df[col]
            .replace(to_replace=r"^\s*(nan|none|null)\s*$", value=pd.NA, regex=True)
            .apply(lambda x: pd.NA if pd.isna(x) or str(x).strip() == "" else str(x).strip())
        )

    list_cols = ["activity_places", "top_suppliers", "main_customers"]
    for col in list_cols:
        if col in corporate_registry_df.columns:
            def to_clean_string(x):
                if x is None or (isinstance(x, float) and pd.isna(x)):
                    return pd.NA
                if isinstance(x, list):
                    items = []
                    for item in x:
                        if item is None or (isinstance(item, float) and pd.isna(item)):
                            continue
                        s = str(item).strip()
                        if not s or s.lower() in {"nan", "none", "null"}:
                            continue
                        items.append(s)
                    return "; ".join(items) if items else pd.NA
                s = str(x).strip()
                if not s or s.lower() in {"nan", "none", "null"}:
                    return pd.NA

                if s.startswith("[") and s.endswith("]") and ("'" in s or '"' in s):
                    inner = s[1:-1].strip()
                    tokens = re.findall(r"'([^']+)'", inner) or re.findall(r"\"([^\"]+)\"", inner)
                    tokens = [t.strip() for t in tokens if t.strip()]
                    return "; ".join(tokens) if tokens else pd.NA

                return s
            corporate_registry_df[col] = corporate_registry_df[col].apply(to_clean_string)
    
    def count_items(value) -> int:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return 0
        if isinstance(value, list):
            return len([v for v in value if v is not None and not (isinstance(v, float) and pd.isna(v)) and str(v).strip()])
        s = str(value).strip()
        if not s or s.lower() in {"nan", "none", "null"}:
            return 0
        if ";" in s:
            return len([p.strip() for p in s.split(";") if p.strip()])
        return 1

    corporate_registry_df["num_activity_places"] = corporate_registry_df["activity_places"].apply(count_items)
    corporate_registry_df["num_suppliers"] = corporate_registry_df["top_suppliers"].apply(count_items)
    corporate_registry_df["num_customers"] = corporate_registry_df["main_customers"].apply(count_items)
    
    corporate_registry_df['processed_date'] = datetime.now().strftime('%Y-%m-%d')
    corporate_registry_df['source'] = 'harmonized'
    
    null_counts = corporate_registry_df.isna().sum().to_dict()
    logger.info(f"Schema harmonized: {len(corporate_registry_df)} records")
    logger.info(
        f"  Match diagnostics (from outer join on name_key): "
        f"matched={matched}, s1_only={s1_only}, s2_only={s2_only}, neither={neither}"
    )
    logger.info(f"  Null counts: {null_counts}")
    return corporate_registry_df
