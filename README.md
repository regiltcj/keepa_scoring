# keepa_scoring

Trend metrics
==================================
The scores under this category consists of two components
1. General trend - whether the feeature was growing, declining or stable throughout the timeframe
2. Consistency of the trend - If the feature has an abrupt growth or consistent growth
	

Following are the features of which we analyse the trend for.
1. daily_sales
2. daily_sales_share
3. daily_items
4. daily_items_share
5. avg_reviews_count
6. avg_reviews_score
7. products
8. pricing

Of the above, features 1-7 are initially analysed for each category with in a brand and then a weighted sum (weihged based on each categories contribution to the total) is used to arrive at the brandwise score.
Feature 8 is analysed for each product within a brand and a similar weighted sum (based on sales) across different products gives us the score for each brand.
Note that while calculating the weightage for each category/product within a brand for weighted sum, only those that has substantial contribution is considered.

Scores are calculated only for those products/categories for which we have data for a given number of days. (Since only then we could analyse the trend)
Threshold value of the above mentioned number of days is computed dynamically depending on the number of days for which ~90% of the products/categories have data for

Periodic scores
==================================
These metrics evaluate the brands based on
1. Competition_Entropy

	Here we first measure the entropy of competition in each category (whether the mean daily_sales in a category is uniformaly distributed across different brands) and provide an entropy score for each category
	Categories which has unifrom distribution (lower entropy) in different brands are given higher score.
	Once the category wise score is computed, brandwise competition entropy score is computed by taking a weighted sum of above score weighed based on the daily_sales in each category

2. Ratio_to_Leader

	Here we identify the top brand based on mean daily_sales in each of the category and compute the ratio of a brand's daily_sales in a category to that of the leader
	Final score is computed as weighted sum of score in each category, weighed based on daily_sales of brand in each category

3. Out_of_Stock

	We compute the mean out_of_stock_pct for the time period and inverse it linearly so that lower the out_of_stock_pct, higher the score
	
4. Product_Concenteration

	This score consist of two parts
		- product concentration - 
			This is a measure of number of products a brands has. Higher the number of products, better the score
			We measure the ratio of mean number of products a brand has in a timeframe to the median of mean number of products of all the brands in that time frame
		- revenue concenteration
			This measures how the total daily_sales of the brand is distributed across its products. If its uniformly distributed, higher would be the score.
			Uniformity is measured based on the entropy with a little modification such that if the brand has only one product, it is given higher entropy
			Entropy is then linearly inversed such that lower entropy (uniform distribution) has a higher score 
	Final score is obtained by multiplying product concenteration and revenue concentration

5. Category_Concenteration

	This score also consist of two parts
		- category concentration - 
			This is a measure of number of category a brands sells product in. Higher the number of categories, better the score
			We measure the ratio of mean number of categories a brand has in a timeframe to the median of mean number of categories of all the brands in that time frame
		- revenue concenteration
			This measures how the total daily_sales of the brand is distributed across its categories. If its uniformly distributed, higher would be the score.
			Uniformity is measured based on the entropy with a little modification such that if the brand sells only in one category, it is given higher entropy
			Entropy is then linearly inversed such that lower entropy (uniform distribution) has a higher score 
	Final score is obtained by multiplying category concenteration and revenue concentration

Snapshot scores
==================================
