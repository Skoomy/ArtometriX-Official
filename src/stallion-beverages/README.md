 ## Data Overview for SKU-Level Demand Forecasting

  Here's what we have in the data/ directory:

  Core Sales Data (21,000 records each)

  1. historical_volume.csv - Primary demand data
  - Granularity: Agency × SKU × Month
  - 25 SKUs (SKU_01 through SKU_34, not sequential)
  - 58 Agencies across different regions
  - Time period: January 2013 to December 2017 (60 months)
  - Target variable: Volume (units sold)

  2. price_sales_promotion.csv - Pricing & promotion data
  - Same granularity (Agency × SKU × Month)
  - Price: Product price per SKU/agency/month
  - Sales: Regular sales amount
  - Promotions: Promotional sales amount
  - Matches historical_volume structure perfectly

  External Factors

  3. event_calendar.csv (61 records)
  - Monthly calendar with 12 event types:
    - Holidays: Easter, Christmas, New Year, Labor Day, Independence Day, Good Friday
    - Events: Regional Games, FIFA U-17 World Cup, Football Gold Cup, Beer Capital, Music Fest, Revolution Day Memorial
  - Binary flags (0/1) for each event by month

  4. weather.csv (3,600 records)
  - Avg_Max_Temp by Agency and Month
  - Weather varies by region (agency-specific)

  5. demographics.csv (60 records)
  - Avg_Population_2017: Population per agency
  - Avg_Yearly_Household_Income_2017: Income level per agency
  - Static demographic features by agency

  6. industry_volume.csv & industry_soda_sales.csv (61 records each)
  - Monthly industry benchmarks
  - Overall market trends for beverages and soda category

  Key Insights for Forecasting

  ✓ Complete SKU-level granularity: 25 SKUs × 58 agencies × 60 months
  ✓ Rich feature set: Price, promotions, weather, events, demographics, industry trends
  ✓ 5 years of history: Good for time series modeling
  ✓ Balanced structure: Price/promotion data aligns with volume data

  This is a solid dataset for SKU-level demand forecasting with plenty of external regressors!