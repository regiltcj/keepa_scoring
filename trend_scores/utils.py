import datetime as dt
import pandas as pd
import scipy.stats as sc


# Function to scale the data
def scale_data(x, scaler):
    # Returns the same data if there are no non-null values in it
    if x[~x.isna()].shape[0] == 0:
        return x
    # Scaling the dataset linearly to the range [0, 1]
    # Minimum value is scaled to 0 while maximum value is scaled to 1
    scaled_x = scaler.fit_transform(x.to_numpy().reshape(-1, 1))
    ser = pd.Series(scaled_x.reshape(-1), index=x.index)
    return ser


# Function to get the growth rate (Growth would be the slope of the line fitted on the actual dataset)
def get_growth_rate(df, field_name):
    # Converting date to normal numerical value
    date_ordinal = df["sample_date"].map(dt.datetime.toordinal)
    # Fitting the line and finding the slope of the line
    slope, intercept, r_value, p_value, std_err = sc.linregress(date_ordinal, df[field_name])
    return slope


# Function to filter the dataframe such that only those that sufficient days of data for trend analysis is retained
def filter_brand_metrics_daily(df, field_name, agg_field, filtered_fields, pct, null_filter):
    # Computing the number of days for which we have either positive or non-null data for the field 'field_name'
    if null_filter:
        pos_val_count = df.groupby(by=["brand", agg_field]).apply(lambda x: sum(x[field_name].isna())).reset_index()
    else:
        pos_val_count = df.groupby(by=["brand", agg_field]).apply(lambda x: sum(x[field_name] > 0)).reset_index()
    # Finding the cumulative sum of the number of brand-'agg_field' combinations that has given number of days data
    pos_val_count_cum = pos_val_count[0].value_counts(normalize=True).sort_index(ascending=False).cumsum()
    # Finding the minimum number of days for which at-least 'pct'% of brand-'agg_field' combinations have data for
    pos_val_days = pos_val_count_cum.loc[pos_val_count_cum > pct].index[0]
    # Filtering only those brand-'agg_field' combination that have at least 'pos_val_days' data
    pos_val_count_filtered = pos_val_count.loc[pos_val_count[0] >= pos_val_days]
    df_filtered = pd.merge(df, pos_val_count_filtered, on=["brand", agg_field], how="inner",
                           validate="many_to_one").loc[:, filtered_fields]
    return df_filtered


# Function to compute the weightage for each 'agg_field' to arrive at the final score for the brand
def category_weight(df, field_name, agg_field):
    weights = df.groupby(by=["brand", agg_field])[field_name].sum().reset_index()
    # Finding the weightage across all the categories of the brand
    weights.loc[:, "weightage"] = weights.groupby(by=["brand"])[field_name].transform(
        lambda x: x / x.sum())
    # Identifying the standard deviation of each group (We are not normalizing it with N-1 to avoid NaN values)
    weights["stddev"] = weights.groupby("brand")["weightage"].transform(lambda x: x.std(ddof=0))
    # Filtering the top contributors.
    # Top Contributor - Categories the weightage is greater than or equal to the standard deviation of the brand
    weights = weights.loc[
        weights["weightage"] >= weights["stddev"], ["brand", agg_field, field_name]]
    weights["weightage"] = weights.groupby(by=["brand"])[field_name].transform(lambda x: x / x.sum())
    return weights


# Function to find the ratio of weeks with positive or stable growth to the total number of weeks
def get_consistency_score(df, field_name, is_threshold):
    threshold = 0
    if is_threshold:
        threshold = 0.1 * df[field_name].max()
    return (df[field_name] > threshold).sum() / df.shape[0]


# Compute the trend score
def compute_trend_score(df, field_name, agg_field):
    scaled_field_name = "scaled_" + field_name
    cols = ["brand", agg_field, "sample_date", field_name, scaled_field_name, "week_number"]
    df_filtered = filter_brand_metrics_daily(df, field_name, agg_field, cols, 0.9, False)

    # Trend score
    score = df_filtered.groupby(by=["brand", agg_field]).apply(get_growth_rate, scaled_field_name).reset_index()
    weightage = category_weight(df_filtered, field_name, agg_field)
    weighted_score = pd.merge(weightage, score, on=["brand", agg_field], how="inner")
    weighted_score["weighted_score"] = weighted_score[0] * weighted_score["weightage"]
    trend_score = weighted_score.groupby(by="brand")["weighted_score"].sum().reset_index()
    trend_score.rename(columns={"weighted_score": "trend_score"}, inplace=True)

    # Consistency score
    weekly_growth = df_filtered.groupby(by=["brand", agg_field, "week_number"]).apply(
        get_growth_rate, scaled_field_name).reset_index()
    weekly_score = weekly_growth.groupby(by=["brand", agg_field]).apply(get_consistency_score, 0, False).reset_index()
    weighted_weekly_score = pd.merge(weightage, weekly_score, on=["brand", agg_field], how="inner")
    weighted_weekly_score["weighted_weekly_score"] = weighted_weekly_score[0] * weighted_weekly_score["weightage"]
    consistency_score = weighted_weekly_score.groupby(by="brand")["weighted_weekly_score"].sum().reset_index()
    consistency_score.rename(columns={"weighted_weekly_score": "consistency_score"}, inplace=True)

    score = pd.merge(trend_score, consistency_score, on="brand", how="inner", validate="one_to_one")
    score["trend_"+field_name] = score["trend_score"] + score["consistency_score"]
    return score.loc[:, ["brand", "trend_"+field_name]]
