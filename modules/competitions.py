from pathlib import Path
import pandas as pd
import utils_wca as uw
import matplotlib.pyplot as plt
from datetime import datetime

def most_competitions(df_competitions):
    top = df_competitions['name'].value_counts().head(10).reset_index()
    top.columns = ['Competition', 'Count']
    return top

def most_countries(df_competitions):
    top = df_competitions['countryId'].value_counts().head(10).reset_index()
    top.columns = ['Country', 'Count']
    return top

def run(db_tables, config, logger):
    # df_comp = db_tables["competitions"]

    # results = {
    #     "Most Competitions": most_competitions(df_comp),
    #     "Most Countries": most_countries(df_comp),
    # }

    # # Create figures dict
    # figures = {}

    # # Example figure
    # fig, ax = plt.subplots(figsize=(8,6))
    # results["Most Competitions"].plot(kind="bar", x="Competition", y="Count", ax=ax)
    # ax.set_title("Top 10 Competitions")
    # plt.tight_layout()
    # figures["top10_competitions"] = fig

    section_name = __name__.split(".")[-1]
    a = uw.import_test(section_name)

    print(a)


    # uw.export_data(results, figures, section_name=section_name, config=config, logger=logger)
