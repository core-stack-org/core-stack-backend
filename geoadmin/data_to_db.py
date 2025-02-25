import os
import pandas as pd
from .models import State, District, Block


def import_data_to_db(state_csv_path):
    state_data = pd.read_csv(state_csv_path)
    
    for state_name, state_census_code in zip(state_data["State name"].str.capitalize().unique(), state_data["State census code"].unique()):
        print('state_name: ', state_name)
        print('state_census_code: ', state_census_code)
        state, created = State.objects.get_or_create(state_name=state_name, state_census_code=state_census_code)
        
        state_district_data = state_data[state_data["State name"].str.capitalize() == state_name]
        unique_districts = state_district_data['District name'].str.capitalize().unique()
        
        for district_name in unique_districts:
            print('district_name: ', district_name)
            district_df = state_district_data[state_district_data['District name'].str.capitalize() == district_name]
            district_census_code = str(district_df['District census code'].iloc[0])
            
            district, created = District.objects.get_or_create(district_name=district_name, district_census_code=district_census_code, state=state)

            unique_blocks = district_df['Subdistrict name'].unique()
            
            for block_name in unique_blocks:
                block_df = district_df[district_df['Subdistrict name'] == block_name]
                block_census_code = str(block_df['Subdistrict census code'].iloc[0])
                Block.objects.get_or_create(block_name=block_name, block_census_code=block_census_code, district=district)

# if __name__=="__main__":
#     csv_path = "/home/ankit/gramvaani/nrm/checkin/backend/nrm-app/data/admin-boundaries/json-data/andhra pradesh.json"
#     import_data_to_db(csv_path)