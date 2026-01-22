import json
import csv
import random
import pandas as pd
import logging
from faker import Faker
from typing import List, Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

fake = Faker()

SUPPLIER_NAMES = [
    "Global Materials Inc", "Tech Components Ltd", "Raw Materials Co",
    "Industrial Supplies Corp", "Manufacturing Partners LLC",
    "Supply Chain Solutions", "Material Sourcing Group", "Component Distributors",
    "Industrial Goods Co", "Supply Network International"
]

CUSTOMER_NAMES = [
    "Retail Giants Corp", "Consumer Products Inc", "Market Leaders Ltd",
    "Distribution Networks", "Retail Partners Group", "Sales Corporation",
    "Commercial Buyers LLC", "Business Customers Co", "Trade Partners Inc",
    "Market Solutions Ltd"
]

ACTIVITY_PLACES = [
    "Manufacturing", "Distribution", "Warehousing", "Logistics",
    "Procurement", "Quality Control", "Shipping", "Inventory Management",
    "Supply Chain Operations", "Material Handling"
]


def generate_source1_data(num_rows: int) -> List[Dict]:
    data = []
    
    for i in range(num_rows):
        corporate_name = fake.company()
        address = fake.address().replace('\n', ', ')
        num_activities = random.randint(2, 5)
        activity_places = random.sample(ACTIVITY_PLACES, num_activities)
        num_suppliers = random.randint(3, 8)
        top_suppliers = random.sample(SUPPLIER_NAMES, min(num_suppliers, len(SUPPLIER_NAMES)))
        
        record = {
            'corporate_name_S1': corporate_name,
            'address': address,
            'activity_places': activity_places,
            'top_suppliers': top_suppliers
        }
        data.append(record)
    
    return data


def generate_source2_data(num_rows: int) -> List[Dict]:
    data = []
    
    for i in range(num_rows):
        corporate_name = fake.company()
        num_customers = random.randint(2, 6)
        main_customers = random.sample(CUSTOMER_NAMES, min(num_customers, len(CUSTOMER_NAMES)))
        revenue = round(random.uniform(10.0, 5000.0), 2)
        profit_margin = random.uniform(0.05, 0.20)
        profit = round(revenue * profit_margin, 2)
        
        record = {
            'corporate_name_S2': corporate_name,
            'main_customers': main_customers,
            'revenue': revenue,
            'profit': profit
        }
        data.append(record)
    
    return data


def save_to_json(data: List[Dict], filename: str):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved {len(data)} records to {filename}")


def save_to_csv(data: List[Dict], filename: str):
    if not data:
        return
    
    fieldnames = list(data[0].keys())
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for record in data:
            csv_record = {}
            for key, value in record.items():
                if isinstance(value, list):
                    csv_record[key] = '; '.join(value)
                else:
                    csv_record[key] = value
            writer.writerow(csv_record)
    
    logger.info(f"Saved {len(data)} records to {filename}")


def save_to_parquet(data: List[Dict], filename: str):
    if not data:
        return
    
    df = pd.DataFrame(data)
    df.to_parquet(filename, index=False, engine='pyarrow', compression='snappy')
    
    logger.info(f"Saved {len(data)} records to {filename}")


def generate_and_save_data(num_rows: int, output_dir: str = '.', output_format: str = 'parquet') -> Dict[str, str]:
    if num_rows <= 0:
        raise ValueError("Number of rows must be greater than 0")
    
    logger.info(f"Generating {num_rows} rows for each source...")
    
    source1_data = generate_source1_data(num_rows)
    source2_data = generate_source2_data(num_rows)
    
    save_json = output_format in ['json', 'both']
    save_csv = output_format in ['csv', 'both']
    save_parquet = output_format in ['parquet', 'both']
    
    generated_files = {}
    
    if save_json:
        file_path = f"{output_dir}/source1_supply_chain.json"
        save_to_json(source1_data, file_path)
        generated_files['source1_json'] = file_path
    
    if save_csv:
        file_path = f"{output_dir}/source1_supply_chain.csv"
        save_to_csv(source1_data, file_path)
        generated_files['source1_csv'] = file_path
    
    if save_parquet:
        file_path = f"{output_dir}/source1_supply_chain.parquet"
        save_to_parquet(source1_data, file_path)
        generated_files['source1_parquet'] = file_path
    
    if save_json:
        file_path = f"{output_dir}/source2_financial.json"
        save_to_json(source2_data, file_path)
        generated_files['source2_json'] = file_path
    
    if save_csv:
        file_path = f"{output_dir}/source2_financial.csv"
        save_to_csv(source2_data, file_path)
        generated_files['source2_csv'] = file_path
    
    if save_parquet:
        file_path = f"{output_dir}/source2_financial.parquet"
        save_to_parquet(source2_data, file_path)
        generated_files['source2_parquet'] = file_path
    
    logger.info(f"Data generation complete!")
    logger.info(f"  - Source 1: {len(source1_data)} records")
    logger.info(f"  - Source 2: {len(source2_data)} records")
    
    return generated_files
