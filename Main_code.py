import sys
import pandas as pd
import numpy as np

def to_string(s):
    if s is None:
        return []
    elif isinstance(s, str):
        return [s]
    elif isinstance(s, (list, tuple, np.ndarray)):
        return [str(item) for item in s]
    else:
        return [str(s)]
    
def clean_duplicate(s1, s2):
    s1 = to_string(s1)
    s2 = to_string(s2)
    s = ' '.join(s1) + ' '.join(s2)
    if s.count(', ') > 0:
        parts = [p.strip() for p in s.split(',')]
        unique_parts = list(dict.fromkeys(parts))
        return ", ".join(unique_parts)

df = pd.read_parquet('veridion_product_deduplication_challenge.snappy.parquet')
df = df.sort_values('unspsc', ascending = True)
record = df.to_records(index = False)
n = record.size
j = 1
a = record[0].unspsc
change = [0]
change_n = 0

for i in range (0, n):
    if record[i].unspsc != a:
        change.append(i)
        a = record[i].unspsc
        change_n = change_n + 1

change.append(n)
change_n = change_n
to_delete = []
b = 0

for index in range(0, change_n):
    
    min_i = change[index]
    max_i = change[index + 1]
    max_j = change[index + 2]
    
    for i in range(min_i, max_i):
        for j in range(i + 1, max_j):
            
            if (record[i].product_name == record[j].product_name and record[i].product_title == record[j].product_title and record[i].eco_friendly == record[j].eco_friendly) :

                if record[i].root_domain != record[j].root_domain:
                    record[i].root_domain = clean_duplicate(record[i].root_domain, record[j].root_domain)

                if record[i].page_url != record[j].page_url:
                    record[i].page_url = clean_duplicate(record[i].page_url, record[j].page_url)              
                
                if to_string(record[i].product_identifier) is not None and to_string(record[j].product_identifier) is not None:
                    if to_string(record[i].product_identifier) != to_string(record[j].product_identifier):
                        record[i].product_identifier = [item1 for item1 in record[i].product_identifier] + [item2 for item2 in record[j].product_identifier]

                if record[i].product_summary is not None and record[j].product_summary is not None:
                    if record[i].product_summary != record[j].product_summary :
                        record[i].product_summary = record[i].product_summary + '. ' + record[j].product_summary  

                if record[i].brand is not None and record[j].brand is not None:
                    if record[i].brand != record[j].brand:
                        record[i].brand = record[i].brand + ', ' + record[j].brand                   
                
                if to_string(record[i].intended_industries) is not None and to_string(record[j].intended_industries) is not None:
                    if to_string(record[i].intended_industries) != to_string(record[j].intended_industries):
                        record[i].intended_industries = clean_duplicate(record[i].intended_industries, record[j].intended_industries)

                if record[i].applicability is not None and record[j].applicability is not None:
                    if to_string(record[i].applicability) != to_string(record[j].applicability):
                        record[i].applicability = clean_duplicate(record[i].applicability, record[j].applicability) 

                if to_string(record[i].production_capacity) is not None and to_string(record[j].production_capacity) is not None:
                    if to_string(record[i].production_capacity) != to_string(record[j].production_capacity):
                        record[i].production_capacity = [item1 for item1 in record[i].production_capacity] + [item2 for item2 in record[j].production_capacity]

                if to_string(record[i].price) is not None and to_string(record[j].price):
                    if to_string(record[i].price) != to_string(record[j].price):
                        record[i].price = [item1 for item1 in record[i].price] + [item2 for item2 in record[j].price] 

                if record[i].materials is not None and record[j].materials is not None:
                    if to_string(record[i].materials) != to_string(record[j].materials):
                        record[i].materials = clean_duplicate(record[i].materials, record[j].materials)  

                if to_string(record[i].ingredients) is not None and to_string(record[j].ingredients) is not None:
                   if to_string(record[i].ingredients) != to_string(record[j].ingredients):
                        record[i].ingredients = clean_duplicate(record[i].ingredients, record[j].ingredients)

                if to_string(record[i].manufacturing_countries) is not None and to_string(record[j].manufacturing_countries) is not None:
                    if to_string(record[i].manufacturing_countries) != to_string(record[j].manufacturing_countries):
                        record[i].manufacturing_countries = clean_duplicate(record[i].manufacturing_countries, record[j].manufacturing_countries)

                if to_string(record[i].manufacturing_type) is not None and to_string(record[j].manufacturing_type) is not None:
                    if to_string(record[i].manufacturing_type) != to_string(record[j].manufacturing_type):
                        record[i].manufacturing_type = clean_duplicate(record[i].manufacturing_type, record[j].manufacturing_type)

                if to_string(record[i].customization) is not None and to_string(record[j].customization) is not None:
                    if to_string(record[i].customization) != to_string(record[j].customization):
                        record[i].customization = clean_duplicate(record[i].customization, record[j].customization)

                if to_string(record[i].packaging_type) is not None and to_string(record[j].packaging_type) is not None:
                    if to_string(record[i].packaging_type) != to_string(record[j].packaging_type):
                        record[i].packaging_type = clean_duplicate(record[i].packaging_type, record[j].packaging_type)

                if to_string(record[i].form) is not None and to_string(record[j].form) is not None:
                    if to_string(record[i].form) != to_string(record[j].form):
                        record[i].form = clean_duplicate(record[i].form , record[j].form)

                if to_string(record[i].purity) is not None and to_string(record[j].purity) is not None:
                    if to_string(record[i].purity) != to_string(record[j].purity):
                        record[i].purity = [item1 for item1 in record[i].purity] + [item2 for item2 in record[j].purity]

                if to_string(record[i].pressure_rating) is not None and to_string(record[j].pressure_rating) is not None:
                    if to_string(record[i].pressure_rating) != to_string(record[j].pressure_rating): 
                        record[i].pressure_rating = [item1 for item1 in record[i].pressure_rating] + [item2 for item2 in record[j].pressure_rating]

                if to_string(record[i].power_rating) is not None and to_string(record[j].power_rating) is not None:
                    if to_string(record[i].power_rating) != to_string(record[j].power_rating):
                        record[i].power_rating  = [item1 for item1 in record[i].power_rating] + [item2 for item2 in record[j].power_rating]

                if to_string(record[i].quality_standards_and_certifications) is not None and to_string(record[j].quality_standards_and_certifications) is not None:
                    if to_string(record[i].quality_standards_and_certifications) != to_string(record[j].quality_standards_and_certifications):
                        record[i].quality_standards_and_certifications = clean_duplicate(record[i].quality_standards_and_certifications, record[j].quality_standards_and_certifications)
                
                if to_string(record[i].miscellaneous_features) is not None and to_string(record[j].miscellaneous_features) is not None:
                    if to_string(record[i].miscellaneous_features) != to_string(record[j].miscellaneous_features):
                        record[i].miscellaneous_features = clean_duplicate(record[i].miscellaneous_features, record[j].miscellaneous_features)

                if record[i].description is not None and record[j].description  is not None:
                    if len(record[i].description) < len(record[j].description):
                        record[i].description = record[j].description 

                if record[i].size != record[j].size:
                    record[i].size = record[i].size + ', ' + record[j].size

                if to_string(record[i].color) is not None and to_string(record[j].color):
                    if to_string(record[i].color) != to_string(record[j].color):
                        record[i].color = [item1 for item1 in record[i].color] + [item2 for item2 in record[j].color]

                if record[i].manufacturing_year != record[j].manufacturing_year:
                    record[i].manufacturing_year = clean_duplicate(record[i].manufacturing_year, record[j].manufacturing_year)

                if record[i].energy_efficiency != record[j].energy_efficiency:
                    record[i].energy_efficiency = clean_duplicate(record[i].energy_efficiency, record[j].energy_efficiency)
                
                if to_string(record[i].ethical_and_sustainability_practices) is not None and to_string(record[j].ethical_and_sustainability_practices) is not None:
                    if to_string(record[i].ethical_and_sustainability_practices) != to_string(record[j].ethical_and_sustainability_practices):
                        record[i].ethical_and_sustainability_practices = clean_duplicate(record[i].ethical_and_sustainability_practices, record[j].ethical_and_sustainability_practices)
 
                to_delete.append(j)



record = np.delete(record, to_delete)

df2 = pd.DataFrame(record)
df2.to_csv('Modified_database.csv')