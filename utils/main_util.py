from decimal import Decimal

def format_decimal(value):
    if value is not None:
        return f"{Decimal(value):.2f}"
    return None

def process_excel_to_db(df, table_name, cursor, logger, update_stmt, insert_stmt, where_clause):
    not_affected, updated, inserted, deleted = 0, 0, 0, 0
    excel_ids = df['ID'].tolist()

    cursor.execute(f"SELECT id FROM {table_name}")
    db_ids = [row[0] for row in cursor.fetchall()]

    ids_to_delete = list(set(db_ids) - set(excel_ids))
    if ids_to_delete:
        cursor.executemany(f"DELETE FROM {table_name} WHERE id = %s", [(id_,) for id_ in ids_to_delete])
        deleted = cursor.rowcount
        logger.info(f"Deleted {deleted} rows: {ids_to_delete}")

    for index, row in df.iterrows():
        where_vals = tuple(row[val] for val in where_clause)
        cursor.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE {' AND '.join([f'{field} = %s' for field in where_clause])}",
            where_vals)
        exists = cursor.fetchone()[0] > 0

        if exists:
            cursor.execute(
                f"SELECT * FROM {table_name} WHERE {' AND '.join([f'{field} = %s' for field in where_clause])}",
                where_vals)
            existing_row = cursor.fetchone()
            existing_data = dict(zip([desc[0] for desc in cursor.description], existing_row))

            if all(existing_data.get(col.lower()) == row[col] for col in df.columns if col.lower() in existing_data):
                not_affected += 1
                logger.debug(f"No changes for Row {index + 1}: {row.to_dict()}")
                continue

            cursor.execute(update_stmt, tuple(row[col] for col in df.columns) + where_vals)
            updated += 1
            logger.info(f"Updated Row {index + 1}: {row.to_dict()} from {existing_data}")
        else:
            cursor.execute(insert_stmt, tuple(row[col] for col in df.columns))
            inserted += 1
            logger.info(f"Inserted New Row {index + 1}: {row.to_dict()}")

    return not_affected, updated, inserted, deleted
