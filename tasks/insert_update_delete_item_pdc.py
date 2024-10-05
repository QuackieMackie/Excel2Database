import pandas as pd

def insert_update_delete_item_pdc(connection, config, logger):
    try:
        excel_file_path = config['EXCEL']['item_data_sheet']
        sheet_name = config['EXCEL']['item_pdc_sheet_name']
        table_name = config['DB_SETTINGS']['item_pdc_table']

        # Load the Excel file into a DataFrame
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name, engine='openpyxl')

        # Ignore comment lines
        df = df[~df['id'].astype(str).str.startswith('<comment>')].copy()
        df.replace("Null", None, inplace=True)

        cursor = connection.cursor()

        # Initialize counters
        not_affected = 0
        updated = 0
        inserted = 0
        deleted = 0

        # Get IDs from Excel sheet and database
        excel_ids = df['id'].tolist()
        cursor.execute(f"SELECT id FROM {table_name}")
        db_ids = [row[0] for row in cursor.fetchall()]

        # Delete IDs not found in the Excel sheet
        ids_to_delete = list(set(db_ids) - set(excel_ids))
        if ids_to_delete:
            cursor.execute(f"DELETE FROM {table_name} WHERE id = ANY (%s)", (ids_to_delete,))
            deleted = cursor.rowcount
            logger.info(f"Deleted {deleted} rows: {ids_to_delete}")

        # Iterate through the DataFrame and update or insert records as needed
        for index, row in df.iterrows():
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE id = %s", (row['id'],))
            exists = cursor.fetchone()[0] > 0

            if exists:
                cursor.execute(f"SELECT * FROM {table_name} WHERE id = %s", (row['id'],))
                existing_row = cursor.fetchone()
                existing_data = dict(zip([desc[0] for desc in cursor.description], existing_row))

                if all(existing_data.get(col.lower()) == row[col] for col in df.columns if
                       col.lower() in existing_data):
                    not_affected += 1
                    logger.debug(f"No changes for Item Row {index + 1}: {row.to_dict()}")
                    continue

                cursor.execute(
                    f"""
                    UPDATE {table_name}
                    SET name = %s, modifier = %s, damagetype = %s, operation = %s
                    WHERE id = %s
                    """,
                    (row['name'], row['modifier'], row['damageType'], row['operation'], row['id'])
                )
                updated += 1
                logger.info(f"Updated Item Row {index + 1}: {row.to_dict()} from {existing_data}")
            else:
                cursor.execute(
                    f"""
                    INSERT INTO {table_name}
                    (id, name, modifier, damagetype, operation)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (row['id'], row['name'], row['modifier'], row['damageType'], row['operation'])
                )
                inserted += 1
                logger.info(f"Inserted New Item Row {index + 1}: {row.to_dict()}")

        # Commit the changes
        connection.commit()
        cursor.close()

        # Log the summary of operations
        logger.info(
            f"Item PDC - Not Affected: {not_affected}, Updated: {updated}, Inserted: {inserted}, Deleted: {deleted}"
        )
    except Exception as e:
        logger.error(f"Error in main process: {e}")
