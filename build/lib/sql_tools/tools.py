import os

import pandas as pd


class Tools:
    def tableToCSV(self, data, tableName, databPath, table=True, database=True):
        if table and database:
            pd.DataFrame(data).to_csv(f"{os.path.basename(databPath)}.{tableName}.csv", index=False)
        elif table:
            pd.DataFrame(data).to_csv(f"{tableName}.csv", index=False)
        elif database:
            pd.DataFrame(data).to_csv(f'{os.path.basename(databPath).replace(".db", "").replace(".db3", "").replace(".sqlite", "").replace(".sqlite3", "")}.csv', index=False)
        else:
            raise AttributeError("One attribute must be provided.")
        return True

        # PENDING INSERT COLUMN NAMES FROM THE TABLE AS HEADERS
            
