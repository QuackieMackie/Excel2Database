import pandas as pd
from utils.main_util import format_decimal

def insert_update_delete_monster_positions(connection, config, logger):
    try:
        excel_file_path = config['EXCEL']['monster_data_sheet']
        sheet_name = config['EXCEL']['monster_positions_sheet_name']
        table_name = config['DB_SETTINGS']['monster_positions_table']

        # Load the Excel file into a DataFrame
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name, engine='openpyxl')

        # Ignore comment lines
        df = df[~df['ID'].astype(str).str.startswith('<comment>')]
        df.replace("Null", None, inplace=True)

        cursor = connection.cursor()

        not_affected = 0
        updated = 0
        inserted = 0
        deleted = 0

        excel_ids = df['ID'].tolist()
        cursor.execute(f"SELECT id FROM {table_name}")
        db_ids = [row[0] for row in cursor.fetchall()]

        ids_to_delete = list(set(db_ids) - set(excel_ids))
        if ids_to_delete:
            cursor.execute(f"DELETE FROM {table_name} WHERE id = ANY (%s)", (ids_to_delete,))
            deleted = cursor.rowcount
            logger.info(f"Deleted {deleted} rows: {ids_to_delete}")

        for index, row in df.iterrows():
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE id = %s", (row['ID'],))
            exists = cursor.fetchone()[0] > 0

            row_data = row.to_dict()
            # Format numeric fields
            for key in ['Motion_X', 'Motion_Y', 'Motion_Z', 'Rotation_Yaw', 'Rotation_Pitch', 'Position_X',
                        'Position_Y',
                        'Position_Z']:
                row_data[key] = format_decimal(row_data.get(key))

            if exists:
                cursor.execute(f"SELECT * FROM {table_name} WHERE id = %s", (row['ID'],))
                existing_row = cursor.fetchone()
                existing_data = dict(zip([desc[0] for desc in cursor.description], existing_row))

                # Format numeric fields for existing_data
                for key in ['motion_x', 'motion_y', 'motion_z', 'rotation_yaw', 'rotation_pitch', 'position_x',
                            'position_y', 'position_z']:
                    existing_data[key] = format_decimal(existing_data.get(key))

                if all(existing_data.get(col.lower()) == row_data[col] for col in row.index if
                       col.lower() in existing_data):
                    not_affected += 1
                    logger.debug(f"No changes for Monster Positions Row {index + 1}: {row_data}")
                    continue

                cursor.execute(
                    f"""
                    UPDATE {table_name}
                    SET uuid = %s, monsterattributeid = %s, motion_x = %s, motion_y = %s, motion_z = %s, 
                        rotation_yaw = %s, rotation_pitch = %s, position_x = %s, position_y = %s, position_z = %s, 
                        dimension = %s, dropid = %s
                    WHERE id = %s
                    """,
                    (
                        row_data['UUID'], row_data['MonsterAttributeId'], row_data['Motion_X'], row_data['Motion_Y'],
                        row_data['Motion_Z'], row_data['Rotation_Yaw'], row_data['Rotation_Pitch'],
                        row_data['Position_X'],
                        row_data['Position_Y'], row_data['Position_Z'], row_data['Dimension'], row_data['DropId'],
                        row_data['ID']
                    )
                )
                updated += 1
                logger.info(f"Updated Monster Positions Row {index + 1}: {row_data} from {existing_data}")
            else:
                cursor.execute(
                    f"""
                    INSERT INTO {table_name}
                    (id, uuid, monsterattributeid, motion_x, motion_y, motion_z, rotation_yaw, rotation_pitch, 
                    position_x, position_y, position_z, dimension, dropid)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row_data['ID'], row_data['UUID'], row_data['MonsterAttributeId'], row_data['Motion_X'],
                        row_data['Motion_Y'], row_data['Motion_Z'], row_data['Rotation_Yaw'],
                        row_data['Rotation_Pitch'],
                        row_data['Position_X'], row_data['Position_Y'], row_data['Position_Z'], row_data['Dimension'],
                        row_data['DropId']
                    )
                )
                inserted += 1
                logger.info(f"Inserted New Monster Positions Row {index + 1}: {row_data}")

        # Commit the changes
        connection.commit()
        cursor.close()

        # Log the summary of operations
        logger.info(
            f"Monster Positions - Not Affected: {not_affected}, Updated: {updated}, Inserted: {inserted}, Deleted: {deleted}"
        )
    except Exception as e:
        logger.error(f"Error in main process: {e}")