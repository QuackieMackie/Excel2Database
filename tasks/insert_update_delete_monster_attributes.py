import pandas as pd
from utils.main_util import process_excel_to_db

def insert_update_delete_monster_attributes(connection, config, logger):
    try:
        excel_file_path = config['EXCEL']['monster_data_sheet']
        sheet_name = config['EXCEL']['monster_attributes_sheet_name']
        table_name = config['DB_SETTINGS']['monster_attributes_table']

        # Load the Excel file into a DataFrame
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name, engine='openpyxl')

        # Ignore comment lines
        df = df[~df['ID'].astype(str).str.startswith('<comment>')]
        df.replace("Null", None, inplace=True)

        # Ensure boolean columns are correctly interpreted as booleans
        bool_columns = ['CustomNameVisible', 'Silent', 'Glowing', 'Invisibility', 'Invulnerable']
        for col in bool_columns:
            if col in df.columns:
                df[col] = df[col].map({'TRUE': True, 'FALSE': False, 1: True, 0: False})

        cursor = connection.cursor()

        # Define SQL statements for updating and inserting records
        update_stmt = f"""
        UPDATE {table_name}
        SET customname = %s, customnamevisible = %s, tags = %s, silent = %s, glowing = %s, invisibility = %s, 
            invulnerable = %s, dropid = %s, monstertype = %s, level = %s, health = %s, armor = %s, basedamage = %s, 
            strength = %s, dexterity = %s, intelligence = %s, vitality = %s, spirit = %s, agility = %s, magicres = %s, 
            critchance = %s, dodgechance = %s
        WHERE id = %s
        """
        insert_stmt = f"""
        INSERT INTO {table_name}
        (id, customname, customnamevisible, tags, silent, glowing, invisibility, invulnerable, dropid, monstertype, level, 
        health, armor, basedamage, strength, dexterity, intelligence, vitality, spirit, agility, magicres, critchance, dodgechance)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        where_clause = ['ID']

        # Process the DataFrame against the database records
        not_affected, updated, inserted, deleted = process_excel_to_db(
            df, table_name, cursor, logger, update_stmt, insert_stmt, where_clause)

        # Commit the changes
        connection.commit()
        cursor.close()

        # Log the summary of operations
        logger.info(
            f"Monster Attributes - Not Affected: {not_affected}, Updated: {updated}, Inserted: {inserted}, Deleted: {deleted}"
        )
    except Exception as e:
        logger.error(f"Error in main process: {e}")
