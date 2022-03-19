import pandas as pd


def get_weighted_value(df, weight_df, field_name, agg_field):
    # Combining df and weight_df table
    df = pd.merge(df, weight_df, on="product_id", how="inner", validate="many_to_one")
    # Finding the weights for each product based on its contribution to the brand
    df["weightage"] = df.groupby(by="brand")[agg_field].transform(lambda x: x / x.sum() if x.sum() > 0 else 0)
    # Calculating weighted sum of the sellers
    df["weighted_" + field_name] = df[field_name] * df["weightage"]
    df = df.groupby(by="brand").apply(
        lambda x: x["weighted_" + field_name].sum() if x["weighted_" + field_name].sum() > 0 else x[field_name].mean()
    ).reset_index()
    return df
