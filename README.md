# SkillCorner-Match-Data-ETL
Pulling SkillCorner match -event- data and loading an output schema to be consumed by data analysts/scientists

1. provide all desired match data in JSON format in the same directory
2. setup your db credentials in db.py file
3. run Main.ipynb
4. match data would be processed and written in db, also a csv output will be available 
    locally to check, you can disable this in Main.py cmd: "df.to_csv(f'{table_name}.csv',index= False, mode = 'a')"