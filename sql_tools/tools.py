import os

import pandas as pd


class Tools:
    def tableToCSV(self, data, tableName, databPath):
        pd.DataFrame(data).to_csv(f"{os.path.basename(databPath)}.{tableName}.csv", index=False)
        return True

        # PENDING INSERT COLUMN NAMES FROM THE TABLE AS HEADERS
            
