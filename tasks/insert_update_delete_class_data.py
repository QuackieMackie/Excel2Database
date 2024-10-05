import pandas as pd
from decimal import Decimal
from utils.main_util import format_decimal


def insert_update_delete_class_data(connection, config, logger):
    try:
        excel_file_path = config['EXCEL']['class_data_sheet']
        sheet_name = config['EXCEL']['class_data_sheet_name']
        table_name = config['DB_SETTINGS']['class_data_table']

        # Load Excel file into DataFrame
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name, engine='openpyxl')

        df.replace("Null", None, inplace=True)
        decimal_cols = ['BaseDamage', 'CritChance', 'DodgeChance']
        for col in decimal_cols:
            if col in df.columns:
                df[col] = df[col].apply(format_decimal)

        cursor = connection.cursor()
        not_affected = 0
        updated = 0
        inserted = 0
        deleted = 0

        excel_class_level_pairs = list(zip(df['ClassName'].tolist(), df['Level'].tolist()))
        cursor.execute(f"SELECT classname, level FROM {table_name}")
        db_class_level_pairs = [(row[0], row[1]) for row in cursor.fetchall()]

        pairs_to_delete = list(set(db_class_level_pairs) - set(excel_class_level_pairs))
        for class_name, level in pairs_to_delete:
            cursor.execute(f"DELETE FROM {table_name} WHERE classname = %s AND level = %s", (class_name, level))
        deleted += len(pairs_to_delete)
        if pairs_to_delete:
            logger.info(f"Deleted {deleted} rows: {pairs_to_delete}")

        for index, row in df.iterrows():
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE classname = %s AND level = %s",
                           (row['ClassName'], row['Level']))
            exists = cursor.fetchone()[0] > 0

            if exists:
                cursor.execute(f"SELECT * FROM {table_name} WHERE classname = %s AND level = %s",
                               (row['ClassName'], row['Level']))
                existing_row = cursor.fetchone()
                existing_data = dict(zip([desc[0] for desc in cursor.description], existing_row))

                comparison_data = {
                    col.lower(): Decimal(existing_data[col.lower()]) if col.lower() in decimal_cols else existing_data[
                        col.lower()] for col in df.columns if col.lower() in existing_data}
                input_data = {col.lower(): Decimal(row[col]) if col in decimal_cols else row[col] for col in df.columns
                              if col.lower() in existing_data}

                if comparison_data == input_data:
                    not_affected += 1
                    logger.debug(f"No changes for Class Row {index + 1}: {row.to_dict()}")
                    continue

                cursor.execute(
                    f"""
                    UPDATE {table_name}
                    SET exp = %s, health = %s, mana = %s, basedamage = %s, strength = %s, dexterity = %s, 
                        constitution = %s, intelligence = %s, armor = %s, magicres = %s, critchance = %s, dodgechance = %s
                    WHERE classname = %s AND level = %s
                    """,
                    (
                        row['Exp'], row['Health'], row['Mana'], format_decimal(row['BaseDamage']), row['Strength'],
                        row['Dexterity'], row['Constitution'], row['Intelligence'], row['Armor'],
                        row['MagicRes'], format_decimal(row['CritChance']), format_decimal(row['DodgeChance']),
                        row['ClassName'], row['Level']
                    )
                )
                updated += 1
                logger.info(f"Updated Class Row {index + 1}: {row.to_dict()} from {existing_data}")
            else:
                cursor.execute(
                    f"""
                    INSERT INTO {table_name}
                    (classname, level, exp, health, mana, basedamage, strength, dexterity, 
                     constitution, intelligence, armor, magicres, critchance, dodgechance)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row['ClassName'], row['Level'], row['Exp'], row['Health'], row['Mana'],
                        format_decimal(row['BaseDamage']),
                        row['Strength'], row['Dexterity'], row['Constitution'], row['Intelligence'], row['Armor'],
                        row['MagicRes'], format_decimal(row['CritChance']), format_decimal(row['DodgeChance'])
                    )
                )
                inserted += 1
                logger.info(f"Inserted New Class Row {index + 1}: {row.to_dict()}")

        # Commit changes and close the cursor
        connection.commit()
        cursor.close()

        # Log summary of operations
        logger.info(
            f"Class Data - Not Affected: {not_affected}, Updated: {updated}, Inserted: {inserted}, Deleted: {deleted}"
        )
    except Exception as e:
        logger.error(f"Error processing class data: {e}")
