import pandas as pd

def insert_update_delete_item_data(connection, config, logger):
    try:
        excel_file_path = config['EXCEL']['item_data_sheet']
        sheet_name = config['EXCEL']['item_data_sheet_name']
        table_name = config['DB_SETTINGS']['item_data_table']

        # Load the Excel file into a DataFrame
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name, engine='openpyxl')

        # Ignore comment lines
        df = df[~df['uuid'].astype(str).str.startswith('<comment>')].copy()
        df.replace("Null", None, inplace=True)

        # Ensure 'count' is converted to integers
        if 'count' in df.columns:
            df['count'] = df['count'].astype(int)

        cursor = connection.cursor()

        # Initialize counters
        not_affected = 0
        updated = 0
        inserted = 0
        deleted = 0

        # Get UUIDs from Excel sheet and database
        excel_uuids = df['uuid'].tolist()
        cursor.execute(f"SELECT uuid FROM {table_name}")
        db_uuids = [row[0] for row in cursor.fetchall()]

        # Delete UUIDs not found in the Excel sheet
        ids_to_delete = list(set(db_uuids) - set(excel_uuids))
        if ids_to_delete:
            cursor.execute(f"DELETE FROM {table_name} WHERE uuid = ANY (%s)", (ids_to_delete,))
            deleted = cursor.rowcount
            logger.info(f"Deleted {deleted} rows: {ids_to_delete}")

        # Iterate through the DataFrame and update or insert records as needed
        for index, row in df.iterrows():
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE uuid = %s", (row['uuid'],))
            exists = cursor.fetchone()[0] > 0

            if exists:
                cursor.execute(f"SELECT * FROM {table_name} WHERE uuid = %s", (row['uuid'],))
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
                    SET id = %s, count = %s, itemname = %s, lore = %s, customenchants = %s, damagetype = %s
                    WHERE uuid = %s
                    """,
                    (row['id'], row['count'], row['itemName'], row['lore'], row['customEnchants'], row['damageType'],
                     row['uuid'])
                )
                updated += 1
                logger.info(f"Updated Item Row {index + 1}: {row.to_dict()} from {existing_data}")
            else:
                cursor.execute(
                    f"""
                    INSERT INTO {table_name}
                    (uuid, id, count, itemname, lore, customenchants, damagetype)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (row['uuid'], row['id'], row['count'], row['itemName'], row['lore'], row['customEnchants'],
                     row['damageType'])
                )
                inserted += 1
                logger.info(f"Inserted New Item Row {index + 1}: {row.to_dict()}")

        # Commit the changes
        connection.commit()
        cursor.close()

        # Log the summary of operations
        logger.info(
            f"Item Data - Not Affected: {not_affected}, Updated: {updated}, Inserted: {inserted}, Deleted: {deleted}"
        )
    except Exception as e:
        logger.error(f"Error in main process: {e}")