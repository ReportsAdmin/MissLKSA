select *,'MissL' Halo_Country from
(
       
                select
                10 KPI_ID,
                1 LAYER,
                'total_item_price_auth' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                20 KPI_ID,
                1 LAYER,
                'orders_auth' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                30 KPI_ID,
                1 LAYER,
                'sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                40 KPI_ID,
                1 LAYER,
                'total_ad_revenue_auth' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Paid Revenue' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                50 KPI_ID,
                1 LAYER,
                'total_non_ad_revenue_auth' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Unpaid Revenue' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                60 KPI_ID,
                1 LAYER,
                'spend' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Ad Spend' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                70 KPI_ID,
                1 LAYER,
                'New_customers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'New Customers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                80 KPI_ID,
                1 LAYER,
                'New_customer_Revenue' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'New Customer Revenue' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                90 KPI_ID,
                1 LAYER,
                'Existing_customer_Revenue' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Existing Customer Revenue' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                100 KPI_ID,
                1 LAYER,
                'Bounce' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number of Bounces' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                110 KPI_ID,
                1 LAYER,
                'Productviews' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Product Views' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                120 KPI_ID,
                1 LAYER,
                'Addstocart' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Add to Cart' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                130 KPI_ID,
                1 LAYER,
                'Checkout' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Checkout' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                140 KPI_ID,
                1 LAYER,
                'Shipping' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Shipping' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                150 KPI_ID,
                1 LAYER,
                'PaymentOption' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Payment Option' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                160 KPI_ID,
                1 LAYER,
                'PaymentClick' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Payment Click' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                170 KPI_ID,
                1 LAYER,
                'Transactions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Sessions with Transactions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                180 KPI_ID,
                1 LAYER,
                'visitors' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Visitors' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                190 KPI_ID,
                1 LAYER,
                'registered_users' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Registerd Users' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                200 KPI_ID,
                1 LAYER,
                'New_Customer_Orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'New Customer Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                210 KPI_ID,
                1 LAYER,
                'Existing_Customer_Orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Existing Customer Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                220 KPI_ID,
                1 LAYER,
                'Paid_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Paid Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                230 KPI_ID,
                1 LAYER,
                'Paid_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Paid Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                240 KPI_ID,
                1 LAYER,
                'Non_Paid_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Unpaid Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                250 KPI_ID,
                1 LAYER,
                'Non_Paid_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Unpaid Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                260 KPI_ID,
                1 LAYER,
                'orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Overall Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                270 KPI_ID,
                1 LAYER,
                'DiscountedSales' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Sales with discount' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                280 KPI_ID,
                1 LAYER,
                'NonDiscountedSales' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Sales without discount' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                290 KPI_ID,
                1 LAYER,
                'Sessions_F' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseFacebookKPIs' KPI_SQL,
                'Facebook Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                300 KPI_ID,
                1 LAYER,
                'Impressions_F' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseFacebookKPIs' KPI_SQL,
                'Facebook Impressions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                310 KPI_ID,
                1 LAYER,
                'Adclicks_F' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseFacebookKPIs' KPI_SQL,
                'Facebook Ad Clicks' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                320 KPI_ID,
                1 LAYER,
                'Addtocarts_F' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseFacebookKPIs' KPI_SQL,
                'Facebook Add to Cart' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                330 KPI_ID,
                1 LAYER,
                'Bounces_F' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseFacebookKPIs' KPI_SQL,
                'Facebook number of bounces' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                340 KPI_ID,
                1 LAYER,
                'Spend_F' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseFacebookKPIs' KPI_SQL,
                'Facebook Ad Spend' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                350 KPI_ID,
                1 LAYER,
                'Transactions_F' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseFacebookKPIs' KPI_SQL,
                'Facebook Transactions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                360 KPI_ID,
                1 LAYER,
                'Revenue_F' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseFacebookKPIs' KPI_SQL,
                'Facebook Revenue' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                370 KPI_ID,
                1 LAYER,
                'Sessions_G' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseGoogleKPIs' KPI_SQL,
                'Google Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                380 KPI_ID,
                1 LAYER,
                'Impressions_G' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseGoogleKPIs' KPI_SQL,
                'Google Impressions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                390 KPI_ID,
                1 LAYER,
                'Adclicks_G' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseGoogleKPIs' KPI_SQL,
                'Google Ad Clicks' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                400 KPI_ID,
                1 LAYER,
                'Addtocarts_G' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseGoogleKPIs' KPI_SQL,
                'Google Add to carts' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                410 KPI_ID,
                1 LAYER,
                'Bounces_G' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseGoogleKPIs' KPI_SQL,
                'Google bounces' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                420 KPI_ID,
                1 LAYER,
                'Spend_G' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseGoogleKPIs' KPI_SQL,
                'Google Ad Spend' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                430 KPI_ID,
                1 LAYER,
                'Transactions_G' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseGoogleKPIs' KPI_SQL,
                'Google Transactions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                440 KPI_ID,
                1 LAYER,
                'Revenue_G' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fBaseGoogleKPIs' KPI_SQL,
                'Google Revenue' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                450 KPI_ID,
                3 LAYER,
                'AOV' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'total_item_price_auth,orders_auth' KPI_DEPENDENTS,
                'safe_divide(total_item_price_auth,orders_auth)' KPI_CALC,
                'safe_divide(total_item_price_auth_prevX,orders_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(total_item_price_auth_PrevyearsameX,orders_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                460 KPI_ID,
                3 LAYER,
                'conversion_rate_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Conversion Rate (%)' KPI_DESCRIPTION,
                'orders_auth,sessions' KPI_DEPENDENTS,
                'safe_divide(orders_auth,sessions)' KPI_CALC,
                'safe_divide(orders_auth_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(orders_auth_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                470 KPI_ID,
                3 LAYER,
                'ROAS' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Return on Ad Spend' KPI_DESCRIPTION,
                'total_ad_revenue_auth,spend' KPI_DEPENDENTS,
                'safe_divide(total_ad_revenue_auth,spend)' KPI_CALC,
                'safe_divide(total_ad_revenue_auth_prevX,spend_prevX)' KPI_CALC_PREVX,
                'safe_divide(total_ad_revenue_auth_PrevyearsameX,spend_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                480 KPI_ID,
                3 LAYER,
                'CAC' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Customer Acquisition Cost' KPI_DESCRIPTION,
                'spend,New_customers' KPI_DEPENDENTS,
                'safe_divide(spend,New_customers)' KPI_CALC,
                'safe_divide(Spend_prevX,New_customers_prevX)' KPI_CALC_PREVX,
                'safe_divide(Spend_PrevyearsameX,New_customers_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                490 KPI_ID,
                3 LAYER,
                'productviews_cart_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Product Views to Cart (%)' KPI_DESCRIPTION,
                'Addstocart,Productviews' KPI_DEPENDENTS,
                'safe_divide(Addstocart,Productviews)' KPI_CALC,
                'safe_divide(Addstocart_prevX,Productviews_prevX)' KPI_CALC_PREVX,
                'safe_divide(Addstocart_PrevyearsameX,Productviews_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                500 KPI_ID,
                3 LAYER,
                'cart_checkout_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Cart to Checkout (%)' KPI_DESCRIPTION,
                'Checkout,Addstocart' KPI_DEPENDENTS,
                'safe_divide(Checkout,Addstocart)' KPI_CALC,
                'safe_divide(Checkout_prevX,Addstocart_prevX)' KPI_CALC_PREVX,
                'safe_divide(Checkout_PrevyearsameX,Addstocart_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                510 KPI_ID,
                3 LAYER,
                'checkout_conversion_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Checkout Conversion (%)' KPI_DESCRIPTION,
                'Transactions,Checkout' KPI_DEPENDENTS,
                'safe_divide(Transactions,Checkout)' KPI_CALC,
                'safe_divide(Transactions_prevX,Checkout_prevX)' KPI_CALC_PREVX,
                'safe_divide(Transactions_PrevyearsameX,Checkout_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                520 KPI_ID,
                3 LAYER,
                'login_shipping_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Login to shipping (%)' KPI_DESCRIPTION,
                'Shipping,Checkout' KPI_DEPENDENTS,
                'safe_divide(Shipping,Checkout)' KPI_CALC,
                'safe_divide(Shipping_prevX,Checkout_prevX)' KPI_CALC_PREVX,
                'safe_divide(Shipping_PrevyearsameX,Checkout_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                530 KPI_ID,
                3 LAYER,
                'shipping_payment_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Shipping to Payment (%)' KPI_DESCRIPTION,
                'PaymentClick,Shipping' KPI_DEPENDENTS,
                'safe_divide(PaymentClick,Shipping)' KPI_CALC,
                'safe_divide(PaymentClick_prevX,Shipping_prevX)' KPI_CALC_PREVX,
                'safe_divide(PaymentClick_PrevyearsameX,Shipping_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                540 KPI_ID,
                3 LAYER,
                'payment_txn_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Payment to Transaction (%)' KPI_DESCRIPTION,
                'Transactions,PaymentClick' KPI_DEPENDENTS,
                'safe_divide(Transactions,PaymentClick)' KPI_CALC,
                'safe_divide(Transactions_prevX,PaymentClick_prevX)' KPI_CALC_PREVX,
                'safe_divide(Transactions_PrevyearsameX,PaymentClick_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                550 KPI_ID,
                3 LAYER,
                'bounce_rate_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Bounce Rate (%)' KPI_DESCRIPTION,
                'Bounce,sessions' KPI_DEPENDENTS,
                'safe_divide(Bounce,sessions)' KPI_CALC,
                'safe_divide(Bounce_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Bounce_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                560 KPI_ID,
                3 LAYER,
                'paid_conversion_rate_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Paid Conversion Rate(%)' KPI_DESCRIPTION,
                'Paid_orders,Paid_sessions' KPI_DEPENDENTS,
                'safe_divide(Paid_orders,Paid_sessions)' KPI_CALC,
                'safe_divide(Paid_orders_prevX,Paid_sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Paid_orders_PrevyearsameX,Paid_sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                570 KPI_ID,
                3 LAYER,
                'unpaid_conversion_rate_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Unpaid Conversion Rate (%)' KPI_DESCRIPTION,
                'Non_Paid_orders,Non_Paid_sessions' KPI_DEPENDENTS,
                'safe_divide(Non_Paid_orders,Non_Paid_sessions)' KPI_CALC,
                'safe_divide(Non_Paid_orders_prevX,Non_Paid_sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Non_Paid_orders_PrevyearsameX,Non_Paid_sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                580 KPI_ID,
                3 LAYER,
                'margin_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Margin(%)' KPI_DESCRIPTION,
                'DiscountedSales,NonDiscountedSales' KPI_DEPENDENTS,
                'safe_divide(DiscountedSales,NonDiscountedSales)' KPI_CALC,
                'safe_divide(DiscountedSales_prevX,NonDiscountedSales_prevX)' KPI_CALC_PREVX,
                'safe_divide(DiscountedSales_PrevyearsameX,NonDiscountedSales_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                590 KPI_ID,
                3 LAYER,
                'OtherOrders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Other Orders' KPI_DESCRIPTION,
                'OrdersAllstatus,Transactions' KPI_DEPENDENTS,
                'OrdersAllstatus-Transactions' KPI_CALC,
                'OrdersAllstatus_prevX-Transactions_prevX' KPI_CALC_PREVX,
                'OrdersAllstatus_PrevyearsameX-Transactions_PrevyearsameX' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                600 KPI_ID,
                3 LAYER,
                'AOV_F' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Facebook Average Order Value' KPI_DESCRIPTION,
                'Revenue_F,Transactions_F' KPI_DEPENDENTS,
                'safe_divide(Revenue_F,Transactions_F)' KPI_CALC,
                'safe_divide(Revenue_F_prevX,Transactions_F_prevX)' KPI_CALC_PREVX,
                'safe_divide(Revenue_F_PrevyearsameX,Transactions_F_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                610 KPI_ID,
                3 LAYER,
                'conversion_rate_F_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Facebook Conversion Rate (%)' KPI_DESCRIPTION,
                'Transactions_F,Sessions_F' KPI_DEPENDENTS,
                'safe_divide(Transactions_F,Sessions_F)' KPI_CALC,
                'safe_divide(Transactions_F_prevX,Sessions_F_prevX)' KPI_CALC_PREVX,
                'safe_divide(Transactions_F_PrevyearsameX,Sessions_F_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                620 KPI_ID,
                3 LAYER,
                'ROAS_F' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Facebook Return on Ad Spend' KPI_DESCRIPTION,
                'Revenue_F,Spend_F' KPI_DEPENDENTS,
                'safe_divide(Revenue_F,Spend_F)' KPI_CALC,
                'safe_divide(Revenue_F_prevX,Spend_F_prevX)' KPI_CALC_PREVX,
                'safe_divide(Revenue_F_PrevyearsameX,Spend_F_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                630 KPI_ID,
                3 LAYER,
                'CTR_F_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Facebook CTR (%)' KPI_DESCRIPTION,
                'Adclicks_F,Impressions_F' KPI_DEPENDENTS,
                'safe_divide(Adclicks_F,Impressions_F)' KPI_CALC,
                'safe_divide(Adclicks_F_prevX,Impressions_F_prevX)' KPI_CALC_PREVX,
                'safe_divide(Adclicks_F_PrevyearsameX,Impressions_F_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                640 KPI_ID,
                3 LAYER,
                'add_to_cart_ratio_F_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Facebook Add to Cart (%)' KPI_DESCRIPTION,
                'Addtocarts_F,Sessions_F' KPI_DEPENDENTS,
                'safe_divide(Addtocarts_F,Sessions_F)' KPI_CALC,
                'safe_divide(Addtocarts_F_prevX,Sessions_F_prevX)' KPI_CALC_PREVX,
                'safe_divide(Addtocarts_F_PrevyearsameX,Sessions_F_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                650 KPI_ID,
                3 LAYER,
                'cost_per_click_F' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Facebook Cost per click' KPI_DESCRIPTION,
                'Spend_F,Adclicks_F' KPI_DEPENDENTS,
                'safe_divide(Spend_F,Adclicks_F)' KPI_CALC,
                'safe_divide(Spend_F_prevX,Adclicks_F_prevX)' KPI_CALC_PREVX,
                'safe_divide(Spend_F_PrevyearsameX,Adclicks_F_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                660 KPI_ID,
                3 LAYER,
                'CostPerSession_F' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Facebook Cost per session' KPI_DESCRIPTION,
                'Spend_F,Sessions_F' KPI_DEPENDENTS,
                'safe_divide(Spend_F,Sessions_F)' KPI_CALC,
                'safe_divide(Spend_F_prevX,Sessions_F_prevX)' KPI_CALC_PREVX,
                'safe_divide(Spend_F_PrevyearsameX,Sessions_F_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                670 KPI_ID,
                3 LAYER,
                'CostPerTransaction_F' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Facebook Cost per transaction' KPI_DESCRIPTION,
                'Spend_F,Transactions_F' KPI_DEPENDENTS,
                'safe_divide(Spend_F,Transactions_F)' KPI_CALC,
                'safe_divide(Spend_F_prevX,Transactions_F_prevX)' KPI_CALC_PREVX,
                'safe_divide(Spend_F_PrevyearsameX,Transactions_F_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                680 KPI_ID,
                3 LAYER,
                'bounce_rate_F_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Facebook Bounce rate' KPI_DESCRIPTION,
                'Bounces_F,Sessions_F' KPI_DEPENDENTS,
                'safe_divide(Bounces_F,Sessions_F)' KPI_CALC,
                'safe_divide(Bounces_F_prevX,Sessions_F_prevX)' KPI_CALC_PREVX,
                'safe_divide(Bounces_F_PrevyearsameX,Sessions_F_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                690 KPI_ID,
                3 LAYER,
                'AOV_G' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Google Average Order Value' KPI_DESCRIPTION,
                'Revenue_G,Transactions_G' KPI_DEPENDENTS,
                'safe_divide(Revenue_G,Transactions_G)' KPI_CALC,
                'safe_divide(Revenue_G_prevX,Transactions_G_prevX)' KPI_CALC_PREVX,
                'safe_divide(Revenue_G_PrevyearsameX,Transactions_G_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                700 KPI_ID,
                3 LAYER,
                'conversion_rate_G_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Google Conversion Rate' KPI_DESCRIPTION,
                'Transactions_G,Sessions_G' KPI_DEPENDENTS,
                'safe_divide(Transactions_G,Sessions_G)' KPI_CALC,
                'safe_divide(Transactions_G_prevX,Sessions_G_prevX)' KPI_CALC_PREVX,
                'safe_divide(Transactions_G_PrevyearsameX,Sessions_G_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                710 KPI_ID,
                3 LAYER,
                'ROAS_G' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Google Return on Ad Spend' KPI_DESCRIPTION,
                'google_cust_revenue,google_cust_spend' KPI_DEPENDENTS,
                'safe_divide(google_cust_revenue,google_cust_spend)' KPI_CALC,
                'safe_divide(google_cust_revenue_prevX,google_cust_spend_prevX)' KPI_CALC_PREVX,
                'safe_divide(google_cust_revenue_PrevyearsameX,google_cust_spend_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                720 KPI_ID,
                3 LAYER,
                'CTR_G_PER' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Google CTR (%)' KPI_DESCRIPTION,
                'Adclicks_G,Impressions_G' KPI_DEPENDENTS,
                'safe_divide(Adclicks_G,Impressions_G)' KPI_CALC,
                'safe_divide(Adclicks_G_prevX,Impressions_G_prevX)' KPI_CALC_PREVX,
                'safe_divide(Adclicks_G_PrevyearsameX,Impressions_G_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                730 KPI_ID,
                3 LAYER,
                'add_to_cart_ratio_G_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Google Add to Cart (%)' KPI_DESCRIPTION,
                'Addtocarts_G,Sessions_G' KPI_DEPENDENTS,
                'safe_divide(Addtocarts_G,Sessions_G)' KPI_CALC,
                'safe_divide(Addtocarts_G_prevX,Sessions_G_prevX)' KPI_CALC_PREVX,
                'safe_divide(Addtocarts_G_PrevyearsameX,Sessions_G_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                740 KPI_ID,
                3 LAYER,
                'CostPerClick_G' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Google Cost per click' KPI_DESCRIPTION,
                'Spend_G,Adclicks_G' KPI_DEPENDENTS,
                'safe_divide(Spend_G,Adclicks_G)' KPI_CALC,
                'safe_divide(Spend_G_prevX,Adclicks_G_prevX)' KPI_CALC_PREVX,
                'safe_divide(Spend_G_PrevyearsameX,Adclicks_G_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                750 KPI_ID,
                3 LAYER,
                'CostPerSession_G' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Google Cost per session' KPI_DESCRIPTION,
                'Spend_G,Sessions_G' KPI_DEPENDENTS,
                'safe_divide(Spend_G,Sessions_G)' KPI_CALC,
                'safe_divide(Spend_G_prevX,Sessions_G_prevX)' KPI_CALC_PREVX,
                'safe_divide(Spend_G_PrevyearsameX,Sessions_G_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                760 KPI_ID,
                3 LAYER,
                'CostPerTransaction_G' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Google Cost per transaction' KPI_DESCRIPTION,
                'Spend_G,Transactions_G' KPI_DEPENDENTS,
                'safe_divide(Spend_G,Transactions_G)' KPI_CALC,
                'safe_divide(Spend_G_prevX,Transactions_G_prevX)' KPI_CALC_PREVX,
                'safe_divide(Spend_G_PrevyearsameX,Transactions_G_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                770 KPI_ID,
                3 LAYER,
                'bounce_rate_G_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Google Bounce Rate (%)' KPI_DESCRIPTION,
                'Bounces_G,Sessions_G' KPI_DEPENDENTS,
                'safe_divide(Bounces_G,Sessions_G)' KPI_CALC,
                'safe_divide(Bounces_G_prevX,Sessions_G_prevX)' KPI_CALC_PREVX,
                'safe_divide(Bounces_G_PrevyearsameX,Sessions_G_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                780 KPI_ID,
                1 LAYER,
                'GrossRevenue' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Gross Revenue ' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                790 KPI_ID,
                1 LAYER,
                'NetRevenue' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Net Revenue After Returns ' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                800 KPI_ID,
                1 LAYER,
                'MSRP' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'MSRP' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                810 KPI_ID,
                1 LAYER,
                'ReturnsRevenue' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Returns Revenue' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                820 KPI_ID,
                1 LAYER,
                'total_product_quantity_auth' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sold QTY' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                830 KPI_ID,
                1 LAYER,
                'total_product_quantity' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Units Ordered' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                840 KPI_ID,
                1 LAYER,
                'ReturnedUnits' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Returned Units' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                850 KPI_ID,
                1 LAYER,
                'PaidNetRevenue' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Paid Net Revenue' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                860 KPI_ID,
                1 LAYER,
                'NonPaidNetRevenue' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Unpaid Net Revenue' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                870 KPI_ID,
                1 LAYER,
                'Itemscancelled' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Items cancelled' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                880 KPI_ID,
                3 LAYER,
                'dilution' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Dilution (%)' KPI_DESCRIPTION,
                'MSRP,NetRevenue' KPI_DEPENDENTS,
                'safe_divide((MSRP - NetRevenue),MSRP)' KPI_CALC,
                'safe_divide((MSRP_prevX - NetRevenue_prevX),MSRP_prevX)' KPI_CALC_PREVX,
                'safe_divide((MSRP_PrevyearsameX - NetRevenue_PrevyearsameX),MSRP_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                890 KPI_ID,
                3 LAYER,
                'ATV' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'AOV' KPI_DESCRIPTION,
                'NetRevenue,orders_auth' KPI_DEPENDENTS,
                'safe_divide(NetRevenue,orders_auth)' KPI_CALC,
                'safe_divide(NetRevenue_prevX,orders_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(NetRevenue_PrevyearsameX,orders_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                900 KPI_ID,
                3 LAYER,
                'UPT' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Units Per Order' KPI_DESCRIPTION,
                'total_product_quantity_auth,orders_auth' KPI_DEPENDENTS,
                'safe_divide(total_product_quantity_auth,orders_auth)' KPI_CALC,
                'safe_divide(total_product_quantity_auth_prevX,orders_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(total_product_quantity_auth_PrevyearsameX,orders_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                910 KPI_ID,
                3 LAYER,
                'returns_volume_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Returns Volume (%)' KPI_DESCRIPTION,
                'ReturnedUnits,total_product_quantity_auth' KPI_DEPENDENTS,
                'safe_divide(ReturnedUnits,total_product_quantity_auth)' KPI_CALC,
                'safe_divide(ReturnedUnits_prevX,total_product_quantity_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(ReturnedUnits_PrevyearsameX,total_product_quantity_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                920 KPI_ID,
                3 LAYER,
                'returns_value_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Returns Value (%)' KPI_DESCRIPTION,
                'ReturnsRevenue,NetRevenue' KPI_DEPENDENTS,
                'safe_divide(ReturnsRevenue,NetRevenue)' KPI_CALC,
                'safe_divide(ReturnsRevenue_prevX,NetRevenue_prevX)' KPI_CALC_PREVX,
                'safe_divide(ReturnsRevenue_PrevyearsameX,NetRevenue_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                930 KPI_ID,
                3 LAYER,
                'cancellation_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Cancellation (%)' KPI_DESCRIPTION,
                'Itemscancelled,total_product_quantity' KPI_DEPENDENTS,
                'safe_divide(Itemscancelled,total_product_quantity)' KPI_CALC,
                'safe_divide(Itemscancelled_prevX,total_product_quantity_prevX)' KPI_CALC_PREVX,
                'safe_divide(Itemscancelled_PrevyearsameX,total_product_quantity_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                940 KPI_ID,
                3 LAYER,
                'atv_first_purchase' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'AOV (First Purchase)' KPI_DESCRIPTION,
                'New_customer_Revenue,New_Customer_Orders' KPI_DEPENDENTS,
                'safe_divide(New_customer_Revenue,New_Customer_Orders)' KPI_CALC,
                'safe_divide(New_customer_Revenue_prevX,New_Customer_Orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(New_customer_Revenue_PrevyearsameX,New_Customer_Orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                950 KPI_ID,
                3 LAYER,
                'atv_repeat_purchase' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'AOV (Repeat Purchase)' KPI_DESCRIPTION,
                'Existing_customer_Revenue,Existing_Customer_Orders' KPI_DEPENDENTS,
                'safe_divide(Existing_customer_Revenue,Existing_Customer_Orders)' KPI_CALC,
                'safe_divide(Existing_customer_Revenue_prevX,Existing_Customer_Orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Existing_customer_Revenue_PrevyearsameX,Existing_Customer_Orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                960 KPI_ID,
                3 LAYER,
                'Promotion_Dilution_Per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Promotion Dilution (%)' KPI_DESCRIPTION,
                'total_item_price_auth,GrossRevenue' KPI_DEPENDENTS,
                '1-safe_divide(total_item_price_auth,GrossRevenue)' KPI_CALC,
                '1-safe_divide(total_item_price_auth_prevX,GrossRevenue_prevX)' KPI_CALC_PREVX,
                '1-safe_divide(total_item_price_auth_PrevyearsameX,GrossRevenue_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                970 KPI_ID,
                3 LAYER,
                'Return_Dilution_Per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Return Dilution(%)' KPI_DESCRIPTION,
                'ReturnsRevenue,GrossRevenue' KPI_DEPENDENTS,
                'safe_divide(ReturnsRevenue,GrossRevenue)' KPI_CALC,
                'safe_divide(ReturnsRevenue_prevX,GrossRevenue_prevX)' KPI_CALC_PREVX,
                'safe_divide(ReturnsRevenue_PrevyearsameX,GrossRevenue_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                980 KPI_ID,
                3 LAYER,
                'AUR' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'AUR' KPI_DESCRIPTION,
                'total_item_price_auth,total_product_quantity_auth' KPI_DEPENDENTS,
                'safe_divide(total_item_price_auth,total_product_quantity_auth)' KPI_CALC,
                'safe_divide(total_item_price_auth_prevX,total_product_quantity_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(total_item_price_auth_PrevyearsameX,total_product_quantity_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                990 KPI_ID,
                1 LAYER,
                'Impressions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Impressions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1000 KPI_ID,
                1 LAYER,
                'Clicks' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Clicks' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1010 KPI_ID,
                1 LAYER,
                'ProductDetailViews' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Product Detail Views' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1020 KPI_ID,
                1 LAYER,
                'ProductAddstocart' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Product Add to Cart' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1030 KPI_ID,
                3 LAYER,
                'CTR_Per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CTR' KPI_DESCRIPTION,
                'Clicks,Impressions' KPI_DEPENDENTS,
                'safe_divide(Clicks,Impressions)' KPI_CALC,
                'safe_divide(Clicks_prevX,Impressions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Clicks_PrevyearsameX,Impressions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1040 KPI_ID,
                3 LAYER,
                'Cart_to_Detail_Per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Cart to Detail Rate' KPI_DESCRIPTION,
                'ProductAddstocart,ProductDetailViews' KPI_DEPENDENTS,
                'safe_divide(ProductAddstocart,ProductDetailViews)' KPI_CALC,
                'safe_divide(ProductAddstocart_prevX,ProductDetailViews_prevX)' KPI_CALC_PREVX,
                'safe_divide(ProductAddstocart_PrevyearsameX,ProductDetailViews_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1050 KPI_ID,
                3 LAYER,
                'Buy_to_Detail_Per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Buy to Detail Rate' KPI_DESCRIPTION,
                'orders_auth,ProductDetailViews' KPI_DEPENDENTS,
                'safe_divide(orders_auth,ProductDetailViews)' KPI_CALC,
                'safe_divide(orders_auth_prevX,ProductDetailViews_prevX)' KPI_CALC_PREVX,
                'safe_divide(orders_auth_PrevyearsameX,ProductDetailViews_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1060 KPI_ID,
                1 LAYER,
                'AddstocartPage' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'AddstocartPage' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1070 KPI_ID,
                1 LAYER,
                'LogginPage' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'LogginPage' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1080 KPI_ID,
                1 LAYER,
                'ShippingPage' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'ShippingPage' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1090 KPI_ID,
                1 LAYER,
                'PaymentPage' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'PaymentPage' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1100 KPI_ID,
                3 LAYER,
                'Cart_to_Login' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Cart to login rate' KPI_DESCRIPTION,
                'LogginPage,AddstocartPage' KPI_DEPENDENTS,
                'safe_divide(LogginPage,AddstocartPage)' KPI_CALC,
                'safe_divide(LogginPage_prevX,AddstocartPage_prevX)' KPI_CALC_PREVX,
                'safe_divide(LogginPage_PrevyearsameX,AddstocartPage_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1110 KPI_ID,
                3 LAYER,
                'Login_to_Shipping' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Login to shipping' KPI_DESCRIPTION,
                'ShippingPage,LogginPage' KPI_DEPENDENTS,
                'safe_divide(ShippingPage,LogginPage)' KPI_CALC,
                'safe_divide(ShippingPage_prevX,LogginPage_prevX)' KPI_CALC_PREVX,
                'safe_divide(ShippingPage_PrevyearsameX,LogginPage_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1120 KPI_ID,
                3 LAYER,
                'shipping_to_payment' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Shipping to Payment' KPI_DESCRIPTION,
                'PaymentPage,ShippingPage' KPI_DEPENDENTS,
                'safe_divide(PaymentPage,ShippingPage)' KPI_CALC,
                'safe_divide(PaymentPage_prevX,ShippingPage_prevX)' KPI_CALC_PREVX,
                'safe_divide(PaymentPage_PrevyearsameX,ShippingPage_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1130 KPI_ID,
                3 LAYER,
                'Payment_to_transcation' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Payment to Transcation' KPI_DESCRIPTION,
                'Transactions,PaymentPage' KPI_DEPENDENTS,
                'safe_divide(Transactions,PaymentPage)' KPI_CALC,
                'safe_divide(Transactions_prevX,PaymentPage_prevX)' KPI_CALC_PREVX,
                'safe_divide(Transactions_PrevyearsameX,PaymentPage_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1140 KPI_ID,
                1 LAYER,
                'avgSessionDuration' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Average Session Duration (in sec)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1150 KPI_ID,
                1 LAYER,
                'exits' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'exits' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1160 KPI_ID,
                3 LAYER,
                'ExitRate' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'ExitRate' KPI_DESCRIPTION,
                'exits,sessions' KPI_DEPENDENTS,
                'safe_divide(exits,sessions)' KPI_CALC,
                'safe_divide(exits_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(exits_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1170 KPI_ID,
                3 LAYER,
                'sessions_to_Productviews' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Sessions to Productviews' KPI_DESCRIPTION,
                'Productviews,sessions' KPI_DEPENDENTS,
                'safe_divide(Productviews,sessions)' KPI_CALC,
                'safe_divide(Productviews_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Productviews_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1180 KPI_ID,
                3 LAYER,
                'LP_bounceRate' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Listing Page Bounce Rate' KPI_DESCRIPTION,
                'LP_Lan_Bounce,LP_Lan_Sessions' KPI_DEPENDENTS,
                'safe_divide(LP_Lan_Bounce,LP_Lan_Sessions)' KPI_CALC,
                'safe_divide(LP_Lan_Bounce_prevX,LP_Lan_Sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(LP_Lan_Bounce_PrevyearsameX,LP_Lan_Sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1190 KPI_ID,
                3 LAYER,
                'LP_ConvRate' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Listing Page Conv Rate' KPI_DESCRIPTION,
                'LP_Lan_Transactions,LP_Lan_Sessions' KPI_DEPENDENTS,
                'safe_divide(LP_Lan_Transactions,LP_Lan_sessions)' KPI_CALC,
                'safe_divide(LP_Lan_Transactions_prevX,LP_Lan_Sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(LP_Lan_Transactions_PrevyearsameX,LP_Lan_Sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1200 KPI_ID,
                3 LAYER,
                'H_bounceRate' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Home Page Bounce Rate' KPI_DESCRIPTION,
                'H_Lan_Bounces,H_Lan_Sessions' KPI_DEPENDENTS,
                'safe_divide(H_Lan_Bounces,H_Lan_Sessions)' KPI_CALC,
                'safe_divide(H_Lan_Bounces_prevX,H_Lan_Sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(H_Lan_Bounces_PrevyearsameX,H_Lan_Sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1210 KPI_ID,
                3 LAYER,
                'H_ConvRate' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Home Page Conv Rate' KPI_DESCRIPTION,
                'H_Lan_Sessions,H_Lan_transactions' KPI_DEPENDENTS,
                'safe_divide(H_Lan_transactions,H_Lan_Sessions)' KPI_CALC,
                'safe_divide(H_Lan_transactions_prevX,H_Lan_Sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(H_Lan_transactions_PrevyearsameX,H_Lan_Sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1220 KPI_ID,
                3 LAYER,
                'P_bounceRate' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Product Page Bounce Rate' KPI_DESCRIPTION,
                'P_bounces,P_Sessions' KPI_DEPENDENTS,
                'safe_divide(P_bounces,P_Sessions)' KPI_CALC,
                'safe_divide(P_bounces_prevX,P_Sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(P_bounces_PrevyearsameX,P_Sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1230 KPI_ID,
                3 LAYER,
                'P_ConvRate' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Product Page Conv Rate' KPI_DESCRIPTION,
                'P_Transactions,P_Sessions' KPI_DEPENDENTS,
                'safe_divide(P_Transactions,P_Sessions)' KPI_CALC,
                'safe_divide(P_Transactions_prevX,P_Sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(P_Transactions_PrevyearsameX,P_Sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1240 KPI_ID,
                1 LAYER,
                'H_Sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Home Page Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1250 KPI_ID,
                1 LAYER,
                'H_AvgsesDur' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Home Page Avg Sess Duration' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1260 KPI_ID,
                1 LAYER,
                'H_Pages_Per_Sess' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Home Page Pages Per Session' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1270 KPI_ID,
                1 LAYER,
                'H_AvgPgLT' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Home Page Avg Page Load Time' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1280 KPI_ID,
                3 LAYER,
                'H_bounce_rate' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Home Page Bounce Rate' KPI_DESCRIPTION,
                'H_Lan_Bounces,H_Lan_Sessions' KPI_DEPENDENTS,
                'safe_divide(H_Lan_Bounces,H_Lan_Sessions)' KPI_CALC,
                'safe_divide(H_Lan_Bounces_prevX,H_Lan_sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(H_Lan_Bounces_PrevyearsameX,H_Lan_Sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1290 KPI_ID,
                1 LAYER,
                'H_Bounces' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Home Page Bounces' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1300 KPI_ID,
                3 LAYER,
                'H_Non_Bounce_Sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Home Page Non Bounce Sessions' KPI_DESCRIPTION,
                'H_Sessions,H_Bounces' KPI_DEPENDENTS,
                'H_Sessions-H_Bounces' KPI_CALC,
                'H_Sessions_prevX-H_Bounces_prevX' KPI_CALC_PREVX,
                'H_Sessions_PrevyearsameX-H_Bounces_PrevyearsameX' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1310 KPI_ID,
                3 LAYER,
                'H_Conv_Rate' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Home Page Conversion Rate' KPI_DESCRIPTION,
                'H_Lan_Sessions,H_Lan_transactions' KPI_DEPENDENTS,
                'safe_divide(H_Lan_transactions,H_Lan_Sessions)' KPI_CALC,
                'safe_divide(H_Lan_transactions_prevX,H_Lan_Sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(H_Lan_transactions_PrevyearsameX,H_Lan_Sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1320 KPI_ID,
                1 LAYER,
                'LP_impressions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Listing Page Impressions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1330 KPI_ID,
                1 LAYER,
                'LP_Clicks' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Listing Page Clicks' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1340 KPI_ID,
                1 LAYER,
                'LP_addstocart' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'ListingPage Adds to Cart' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1350 KPI_ID,
                3 LAYER,
                'LP_PvtoCart' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Listing Page ProdDetail to Cart' KPI_DESCRIPTION,
                'LP_addstocart,LP_productDetailViews' KPI_DEPENDENTS,
                'safe_divide(LP_addstocart,LP_productDetailViews)' KPI_CALC,
                'safe_divide(LP_addstocart_prevX,LP_productDetailViews_prevX)' KPI_CALC_PREVX,
                'safe_divide(LP_addstocart_PrevyearsameX,LP_productDetailViews_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1360 KPI_ID,
                1 LAYER,
                'LP_Lan_Bounce' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Listing Page Bounce' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1370 KPI_ID,
                1 LAYER,
                'LP_Lan_Sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Listing Page Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1380 KPI_ID,
                1 LAYER,
                'H_Lan_Bounces' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Home Page Bounce' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1390 KPI_ID,
                1 LAYER,
                'H_Lan_Sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Home Page Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1400 KPI_ID,
                1 LAYER,
                'P_bounces' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Product Page Bounce' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1410 KPI_ID,
                1 LAYER,
                'P_Sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Product Page Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1420 KPI_ID,
                1 LAYER,
                'P_Transactions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Product Page Transactions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1430 KPI_ID,
                1 LAYER,
                'H_Lan_transactions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Home Page Transactions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1440 KPI_ID,
                1 LAYER,
                'LP_Lan_Transactions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Listing Page Trasactions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1450 KPI_ID,
                1 LAYER,
                'LP_addstocart' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Listing Page adds to cart' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1460 KPI_ID,
                1 LAYER,
                'LP_productDetailViews' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Listing Page product detail view' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1470 KPI_ID,
                3 LAYER,
                'Checkout_Conversion_Rate' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Checkout Conversion Rate' KPI_DESCRIPTION,
                'Transactions,Checkout' KPI_DEPENDENTS,
                'safe_divide(Transactions,Checkout)' KPI_CALC,
                'safe_divide(Transactions_prevX,Checkout_prevX)' KPI_CALC_PREVX,
                'safe_divide(Transactions_PrevyearsameX,Checkout_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1480 KPI_ID,
                1 LAYER,
                'tax_amount' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Tax Amount' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1490 KPI_ID,
                3 LAYER,
                'Total_Sales_Value_Without_VAT' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Total Sales Value (Without VAT in SAR)' KPI_DESCRIPTION,
                'total_item_price_auth,tax_amount' KPI_DEPENDENTS,
                'total_item_price_auth-tax_amount' KPI_CALC,
                'total_item_price_auth_prevX-tax_amount_prevX' KPI_CALC_PREVX,
                'total_item_price_auth_PrevyearsameX-tax_amount_PrevyearsameX' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1500 KPI_ID,
                1 LAYER,
                'cancelled_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Cancelled Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1510 KPI_ID,
                3 LAYER,
                'cancellation_order_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Cancelled Orders (%)' KPI_DESCRIPTION,
                'cancelled_orders,orders' KPI_DEPENDENTS,
                'safe_divide(cancelled_orders,orders)' KPI_CALC,
                'safe_divide(cancelled_orders_prevX,orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(cancelled_orders_PrevyearsameX,orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1520 KPI_ID,
                1 LAYER,
                'cod' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'cod_orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1530 KPI_ID,
                1 LAYER,
                'cc' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'cc_orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1540 KPI_ID,
                1 LAYER,
                'returned_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Returned Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1550 KPI_ID,
                3 LAYER,
                'cod_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'COD (%)' KPI_DESCRIPTION,
                'cod,orders_auth' KPI_DEPENDENTS,
                'safe_divide(cod,orders_auth)' KPI_CALC,
                'safe_divide(cod_prevX,orders_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(cod_PrevyearsameX,orders_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1560 KPI_ID,
                3 LAYER,
                'cc_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CC (%)' KPI_DESCRIPTION,
                'cc,orders_auth' KPI_DEPENDENTS,
                'safe_divide(cc,orders_auth)' KPI_CALC,
                'safe_divide(cc_prevX,orders_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(cc_PrevyearsameX,orders_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                7000 KPI_ID,
                1 LAYER,
                'tabby' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'tabby_orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                7010 KPI_ID,
                3 LAYER,
                'tabby_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Tabby (%)' KPI_DESCRIPTION,
                'tabby,orders_auth' KPI_DEPENDENTS,
                'safe_divide(tabby,orders_auth)' KPI_CALC,
                'safe_divide(tabby_prevX,orders_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(tabby_PrevyearsameX,orders_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1570 KPI_ID,
                3 LAYER,
                'returned_orders_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Return Orders (%)' KPI_DESCRIPTION,
                'returned_orders,orders' KPI_DEPENDENTS,
                'safe_divide(returned_orders,orders)' KPI_CALC,
                'safe_divide(returned_orders_prevX,orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(returned_orders_PrevyearsameX,orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1580 KPI_ID,
                3 LAYER,
                'sessions_per_visitors' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Sessions Per User' KPI_DESCRIPTION,
                'sessions,day_level_users' KPI_DEPENDENTS,
                'safe_divide(sessions,day_level_users)' KPI_CALC,
                'safe_divide(sessions_prevX,day_level_users_prevX)' KPI_CALC_PREVX,
                'safe_divide(sessions_PrevyearsameX,day_level_users_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1590 KPI_ID,
                1 LAYER,
                'pageviews' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Page Views' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1600 KPI_ID,
                3 LAYER,
                'pages_per_sessions' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Pageviews Per Session' KPI_DESCRIPTION,
                'pageviews,sessions' KPI_DEPENDENTS,
                'safe_divide(pageviews,sessions)' KPI_CALC,
                'safe_divide(pageviews_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(pageviews_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1610 KPI_ID,
                1 LAYER,
                'mobile' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'mobile' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1620 KPI_ID,
                1 LAYER,
                'tablet' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'tablet' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1630 KPI_ID,
                1 LAYER,
                'desktop' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'desktop' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1640 KPI_ID,
                3 LAYER,
                'mobile_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Mobile Sessions (%)' KPI_DESCRIPTION,
                'mobile,sessions' KPI_DEPENDENTS,
                'safe_divide(mobile,sessions)' KPI_CALC,
                'safe_divide(mobile_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(mobile_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1650 KPI_ID,
                3 LAYER,
                'tablet_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Tablet Sessions (%)' KPI_DESCRIPTION,
                'tablet,sessions' KPI_DEPENDENTS,
                'safe_divide(tablet,sessions)' KPI_CALC,
                'safe_divide(tablet_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(tablet_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1660 KPI_ID,
                3 LAYER,
                'desktop_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Desktop Sessions (%)' KPI_DESCRIPTION,
                'desktop,sessions' KPI_DEPENDENTS,
                'safe_divide(desktop,sessions)' KPI_CALC,
                'safe_divide(desktop_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(desktop_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1670 KPI_ID,
                1 LAYER,
                'newusers_with_age_18_24' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'New Users Age 18-24' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1680 KPI_ID,
                1 LAYER,
                'newusers_with_age_25_34' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'New Users Age 25-34' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1690 KPI_ID,
                1 LAYER,
                'newusers_with_age_35_44' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'New Users Age 35-44' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1700 KPI_ID,
                1 LAYER,
                'newusers_with_age_above_45' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'New Users Age above 45' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1710 KPI_ID,
                1 LAYER,
                'newusers_with_gender_female' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Female New Users' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1720 KPI_ID,
                1 LAYER,
                'newusers_with_gender_male' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Male New Users' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1730 KPI_ID,
                3 LAYER,
                'age_18_24_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                '18-24 Years Old (%)' KPI_DESCRIPTION,
                'newusers_with_age_18_24,age_new_users' KPI_DEPENDENTS,
                'safe_divide(newusers_with_age_18_24,age_new_users)' KPI_CALC,
                'safe_divide(newusers_with_age_18_24_prevX,age_new_users_prevX)' KPI_CALC_PREVX,
                'safe_divide(newusers_with_age_18_24_PrevyearsameX,age_new_users_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1740 KPI_ID,
                3 LAYER,
                'age_25_34_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                '25-34 Years Old (%)' KPI_DESCRIPTION,
                'newusers_with_age_25_34,age_new_users' KPI_DEPENDENTS,
                'safe_divide(newusers_with_age_25_34,age_new_users)' KPI_CALC,
                'safe_divide(newusers_with_age_25_34_prevX,age_new_users_prevX)' KPI_CALC_PREVX,
                'safe_divide(newusers_with_age_25_34_PrevyearsameX,age_new_users_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1750 KPI_ID,
                3 LAYER,
                'age_35_44_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                '35-44 Years Old (%)' KPI_DESCRIPTION,
                'newusers_with_age_35_44,age_new_users' KPI_DEPENDENTS,
                'safe_divide(newusers_with_age_35_44,age_new_users)' KPI_CALC,
                'safe_divide(newusers_with_age_35_44_prevX,age_new_users_prevX)' KPI_CALC_PREVX,
                'safe_divide(newusers_with_age_35_44_PrevyearsameX,age_new_users_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1760 KPI_ID,
                3 LAYER,
                'age_above_45_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Above 45 Years Old (%)' KPI_DESCRIPTION,
                'newusers_with_age_above_45,age_new_users' KPI_DEPENDENTS,
                'safe_divide(newusers_with_age_above_45,age_new_users)' KPI_CALC,
                'safe_divide(newusers_with_age_above_45_prevX,age_new_users_prevX)' KPI_CALC_PREVX,
                'safe_divide(newusers_with_age_above_45_PrevyearsameX,age_new_users_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1770 KPI_ID,
                3 LAYER,
                'gender_male_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Male (%)' KPI_DESCRIPTION,
                'newusers_with_gender_male,gender_new_users' KPI_DEPENDENTS,
                'safe_divide(newusers_with_gender_male,gender_new_users)' KPI_CALC,
                'safe_divide(newusers_with_gender_male_prevX,gender_new_users_prevX)' KPI_CALC_PREVX,
                'safe_divide(newusers_with_gender_male_PrevyearsameX,gender_new_users_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1780 KPI_ID,
                3 LAYER,
                'gender_female_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Female (%)' KPI_DESCRIPTION,
                'newusers_with_gender_female,gender_new_users' KPI_DEPENDENTS,
                'safe_divide(newusers_with_gender_female,gender_new_users)' KPI_CALC,
                'safe_divide(newusers_with_gender_female_prevX,gender_new_users_prevX)' KPI_CALC_PREVX,
                'safe_divide(newusers_with_gender_female_PrevyearsameX,gender_new_users_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1790 KPI_ID,
                1 LAYER,
                'new_visitor_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'New Visitors' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1800 KPI_ID,
                1 LAYER,
                'returning_visitor_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Returning Visitor Users' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1810 KPI_ID,
                3 LAYER,
                'new_visitor_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'New Visitors (% Sessions)' KPI_DESCRIPTION,
                'new_visitor_sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(new_visitor_sessions,sessions)' KPI_CALC,
                'safe_divide(new_visitor_sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(new_visitor_sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1820 KPI_ID,
                3 LAYER,
                'returning_visitor_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Returning Visitors (% Sessions)' KPI_DESCRIPTION,
                'returning_visitor_sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(returning_visitor_sessions,sessions)' KPI_CALC,
                'safe_divide(returning_visitor_sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(returning_visitor_sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1830 KPI_ID,
                1 LAYER,
                'Snapchat_paid_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Snapchat Paid Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1840 KPI_ID,
                1 LAYER,
                'Snapchat_unpaid_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Snapchat Unpaid Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1850 KPI_ID,
                1 LAYER,
                'Facebook_Inhouse_Paid_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Facebook Inhouse Paid Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1860 KPI_ID,
                1 LAYER,
                'Facebook_Dynamic_Remarketing_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Facebook Dynamic Remarketing Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1870 KPI_ID,
                1 LAYER,
                'Facebook_Unpaid_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Facebook Unpaid Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1880 KPI_ID,
                1 LAYER,
                'Instagram_paid_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Instagram Paid Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1900 KPI_ID,
                1 LAYER,
                'Instagram_Unpaid_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Instagram Unpaid Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1910 KPI_ID,
                1 LAYER,
                'influencersrevenue' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Influencers Sales Value (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1930 KPI_ID,
                3 LAYER,
                'Snapchat_paid_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Snapchat Paid (%)' KPI_DESCRIPTION,
                'Snapchat_paid_sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(Snapchat_paid_sessions,sessions)' KPI_CALC,
                'safe_divide(Snapchat_paid_sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Snapchat_paid_sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1940 KPI_ID,
                3 LAYER,
                'Snapchat_unpaid_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Snapchat Unpaid (%)' KPI_DESCRIPTION,
                'Snapchat_unpaid_sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(Snapchat_unpaid_sessions,sessions)' KPI_CALC,
                'safe_divide(Snapchat_unpaid_sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Snapchat_unpaid_sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1950 KPI_ID,
                3 LAYER,
                'Facebook_Inhouse_Paid_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Facebook In-house Paid (%)' KPI_DESCRIPTION,
                'Facebook_Inhouse_Paid_sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(Facebook_Inhouse_Paid_sessions,sessions)' KPI_CALC,
                'safe_divide(Facebook_Inhouse_Paid_sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Facebook_Inhouse_Paid_sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1960 KPI_ID,
                3 LAYER,
                'Facebook_Dynamic_Remarketing_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Facebook Dynamic & Remarketing (%)' KPI_DESCRIPTION,
                'Facebook_Dynamic_Remarketing_sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(Facebook_Dynamic_Remarketing_sessions,sessions)' KPI_CALC,
                'safe_divide(Facebook_Dynamic_Remarketing_sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Facebook_Dynamic_Remarketing_sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1970 KPI_ID,
                3 LAYER,
                'Facebook_Unpaid_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Facebook Unpaid (%)' KPI_DESCRIPTION,
                'Facebook_Unpaid_sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(Facebook_Unpaid_sessions,sessions)' KPI_CALC,
                'safe_divide(Facebook_Unpaid_sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Facebook_Unpaid_sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                1980 KPI_ID,
                3 LAYER,
                'Instagram_paid_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Instagram Paid (%)' KPI_DESCRIPTION,
                'Instagram_paid_sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(Instagram_paid_sessions,sessions)' KPI_CALC,
                'safe_divide(Instagram_paid_sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Instagram_paid_sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2000 KPI_ID,
                3 LAYER,
                'Others_Sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Others (%)' KPI_DESCRIPTION,
                'Others_Sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(Others_Sessions,sessions)' KPI_CALC,
                'safe_divide(Others_Sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Others_Sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2010 KPI_ID,
                3 LAYER,
                'Influencers_codes_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Influencers Sales Value (%)' KPI_DESCRIPTION,
                'influencersrevenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(influencersrevenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(influencersrevenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(influencersrevenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2030 KPI_ID,
                3 LAYER,
                'Unique_Visitors' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Unique Visitors (%)' KPI_DESCRIPTION,
                'visitors,new_visitor_users' KPI_DEPENDENTS,
                'visitors-new_visitor_users' KPI_CALC,
                'visitors_prevX-new_visitor_users_prevX' KPI_CALC_PREVX,
                'visitors_PrevyearsameX-new_visitor_users_PrevyearsameX' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2040 KPI_ID,
                1 LAYER,
                'gender_new_users' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Gender New Users ' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2050 KPI_ID,
                1 LAYER,
                'age_new_users' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Age New Users' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2060 KPI_ID,
                1 LAYER,
                'day_level_new_users' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'New Users' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2070 KPI_ID,
                1 LAYER,
                'day_level_users' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Users' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2080 KPI_ID,
                1 LAYER,
                'SessionDuration' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'SessionDuration' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2090 KPI_ID,
                3 LAYER,
                'avgSessionDuration1' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Session Duration (in sec)' KPI_DESCRIPTION,
                'SessionDuration,sessions' KPI_DEPENDENTS,
                'safe_divide(SessionDuration,sessions)' KPI_CALC,
                'safe_divide(SessionDuration_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(SessionDuration_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2100 KPI_ID,
                1 LAYER,
                'daylevel' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'DayLevel' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2110 KPI_ID,
                3 LAYER,
                'avgdailysales' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Daily Sales (in SAR)' KPI_DESCRIPTION,
                'total_item_price_auth,daylevel' KPI_DEPENDENTS,
                'safe_divide(total_item_price_auth,daylevel)' KPI_CALC,
                'safe_divide(total_item_price_auth_prevX,daylevel_prevX)' KPI_CALC_PREVX,
                'safe_divide(total_item_price_auth_PrevyearsameX,daylevel_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2120 KPI_ID,
                3 LAYER,
                'avgdailyunits' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Daily Units' KPI_DESCRIPTION,
                'total_product_quantity_auth,daylevel' KPI_DEPENDENTS,
                'safe_divide(total_product_quantity_auth,daylevel)' KPI_CALC,
                'safe_divide(total_product_quantity_auth_prevX,daylevel_prevX)' KPI_CALC_PREVX,
                'safe_divide(total_product_quantity_auth_PrevyearsameX,daylevel_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2130 KPI_ID,
                1 LAYER,
                'Total_revenue' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Overall Sales Value (with VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2140 KPI_ID,
                1 LAYER,
                'unsucessful_revenue' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Unsuccessful Sales Value (with VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2150 KPI_ID,
                1 LAYER,
                'influencersorders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Influencers Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2160 KPI_ID,
                3 LAYER,
                'influencersorders_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Influencers Orders (%)' KPI_DESCRIPTION,
                'influencersorders,orders_auth' KPI_DEPENDENTS,
                'safe_divide(influencersorders,orders_auth)' KPI_CALC,
                'safe_divide(influencersorders_prevX,orders_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(influencersorders_PrevyearsameX,orders_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2170 KPI_ID,
                1 LAYER,
                'unsucessful_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Unsuccessful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2180 KPI_ID,
                1 LAYER,
                'sessions_with_age_18_24' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'New Sessions Age 18-24' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2190 KPI_ID,
                1 LAYER,
                'sessions_with_age_25_34' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'New Sessions Age 25-34' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2200 KPI_ID,
                1 LAYER,
                'sessions_with_age_35_44' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'New Sessions Age 35-44' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2210 KPI_ID,
                1 LAYER,
                'sessions_with_age_above_45' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'New Sessions Age above 45' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2220 KPI_ID,
                1 LAYER,
                'sessions_with_gender_female' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Female Sessions ' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2230 KPI_ID,
                1 LAYER,
                'sessions_with_gender_male' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Male Sessions ' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2240 KPI_ID,
                3 LAYER,
                'age_18_24_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                '18-24 Years Old (%)' KPI_DESCRIPTION,
                'sessions_with_age_18_24,age_sessions' KPI_DEPENDENTS,
                'safe_divide(sessions_with_age_18_24,age_sessions)' KPI_CALC,
                'safe_divide(sessions_with_age_18_24_prevX,age_sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(sessions_with_age_18_24_PrevyearsameX,age_sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2250 KPI_ID,
                3 LAYER,
                'age_25_34_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                '25-34 Years Old (%)' KPI_DESCRIPTION,
                'sessions_with_age_25_34,age_sessions' KPI_DEPENDENTS,
                'safe_divide(sessions_with_age_25_34,age_sessions)' KPI_CALC,
                'safe_divide(sessions_with_age_25_34_prevX,age_sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(sessions_with_age_25_34_PrevyearsameX,age_sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2260 KPI_ID,
                3 LAYER,
                'age_35_44_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                '35-44 Years Old (%)' KPI_DESCRIPTION,
                'sessions_with_age_35_44,age_sessions' KPI_DEPENDENTS,
                'safe_divide(sessions_with_age_35_44,age_sessions)' KPI_CALC,
                'safe_divide(sessions_with_age_35_44_prevX,age_sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(sessions_with_age_35_44_PrevyearsameX,age_sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2270 KPI_ID,
                3 LAYER,
                'age_above_45_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Above 45 Years Old (%)' KPI_DESCRIPTION,
                'sessions_with_age_above_45,age_sessions' KPI_DEPENDENTS,
                'safe_divide(sessions_with_age_above_45,age_sessions)' KPI_CALC,
                'safe_divide(sessions_with_age_above_45_prevX,age_sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(sessions_with_age_above_45_PrevyearsameX,age_sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2280 KPI_ID,
                3 LAYER,
                'gender_male_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Male (%)' KPI_DESCRIPTION,
                'sessions_with_gender_male,gender_sessions' KPI_DEPENDENTS,
                'safe_divide(sessions_with_gender_male,gender_sessions)' KPI_CALC,
                'safe_divide(sessions_with_gender_male_prevX,gender_sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(sessions_with_gender_male_PrevyearsameX,gender_sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2290 KPI_ID,
                3 LAYER,
                'gender_female_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Female (%)' KPI_DESCRIPTION,
                'sessions_with_gender_female,gender_sessions' KPI_DEPENDENTS,
                'safe_divide(sessions_with_gender_female,gender_sessions)' KPI_CALC,
                'safe_divide(sessions_with_gender_female_prevX,gender_sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(sessions_with_gender_female_PrevyearsameX,gender_sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2300 KPI_ID,
                1 LAYER,
                'Snapchat_paid_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Snapchat Paid Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2310 KPI_ID,
                1 LAYER,
                'Snapchat_unpaid_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Snapchat Unpaid Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2320 KPI_ID,
                1 LAYER,
                'Facebook_Inhouse_Paid_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Facebook Inhouse Paid Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2330 KPI_ID,
                1 LAYER,
                'Facebook_Dynamic_Remarketing_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Facebook Dynamic Remarketing Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2340 KPI_ID,
                1 LAYER,
                'Facebook_Unpaid_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Facebook Unpaid Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2350 KPI_ID,
                1 LAYER,
                'Instagram_paid_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Instagram Paid Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2360 KPI_ID,
                1 LAYER,
                'Instagram_Unpaid_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Instagram Unpaid Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2370 KPI_ID,
                1 LAYER,
                'TikTok_paid_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'TikTok Paid Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2380 KPI_ID,
                3 LAYER,
                'Snapchat_paid_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Snapchat Paid (%)' KPI_DESCRIPTION,
                'Snapchat_paid_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(Snapchat_paid_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(Snapchat_paid_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(Snapchat_paid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2390 KPI_ID,
                3 LAYER,
                'Snapchat_unpaid_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Snapchat Unpaid (%)' KPI_DESCRIPTION,
                'Snapchat_unpaid_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(Snapchat_unpaid_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(Snapchat_unpaid_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(Snapchat_unpaid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2400 KPI_ID,
                3 LAYER,
                'Facebook_Inhouse_Paid_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Facebook In-house Paid (%)' KPI_DESCRIPTION,
                'Facebook_Inhouse_Paid_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(Facebook_Inhouse_Paid_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(Facebook_Inhouse_Paid_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(Facebook_Inhouse_Paid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2410 KPI_ID,
                3 LAYER,
                'Facebook_Dynamic_Remarketing_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Facebook Dynamic & Remarketing (%)' KPI_DESCRIPTION,
                'Facebook_Dynamic_Remarketing_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(Facebook_Dynamic_Remarketing_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(Facebook_Dynamic_Remarketing_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(Facebook_Dynamic_Remarketing_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2420 KPI_ID,
                3 LAYER,
                'Facebook_Unpaid_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Facebook Unpaid (%)' KPI_DESCRIPTION,
                'Facebook_Unpaid_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(Facebook_Unpaid_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(Facebook_Unpaid_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(Facebook_Unpaid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2430 KPI_ID,
                3 LAYER,
                'Instagram_paid_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Instagram Paid (%)' KPI_DESCRIPTION,
                'Instagram_paid_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(Instagram_paid_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(Instagram_paid_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(Instagram_paid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2440 KPI_ID,
                3 LAYER,
                'Instagram_Unpaid_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Instagram Unpaid (%)' KPI_DESCRIPTION,
                'Instagram_Unpaid_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(Instagram_Unpaid_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(Instagram_Unpaid_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(Instagram_Unpaid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2450 KPI_ID,
                3 LAYER,
                'Others_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Others (%)' KPI_DESCRIPTION,
                'Others_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(Others_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(Others_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(Others_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2460 KPI_ID,
                1 LAYER,
                'newuserssourcemedium' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'New Users Source Medium' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2470 KPI_ID,
                1 LAYER,
                'MissL10_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2480 KPI_ID,
                1 LAYER,
                'Husin10_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2490 KPI_ID,
                1 LAYER,
                'age_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Age Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2500 KPI_ID,
                1 LAYER,
                'gender_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Gender Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2510 KPI_ID,
                1 LAYER,
                'Feda10_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2520 KPI_ID,
                1 LAYER,
                'abrar21_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2530 KPI_ID,
                1 LAYER,
                'feda21_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2540 KPI_ID,
                1 LAYER,
                'roh_n2_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2550 KPI_ID,
                1 LAYER,
                'omseba10_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2560 KPI_ID,
                1 LAYER,
                'marwa10_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2570 KPI_ID,
                1 LAYER,
                'maha_140_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2580 KPI_ID,
                1 LAYER,
                'shahad10_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2590 KPI_ID,
                1 LAYER,
                'MissL10_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2600 KPI_ID,
                1 LAYER,
                'Husin10_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2610 KPI_ID,
                1 LAYER,
                'Feda10_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2620 KPI_ID,
                1 LAYER,
                'abrar21_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2630 KPI_ID,
                1 LAYER,
                'feda21_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2640 KPI_ID,
                1 LAYER,
                'roh_n2_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2650 KPI_ID,
                1 LAYER,
                'omseba10_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2660 KPI_ID,
                1 LAYER,
                'marwa10_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2670 KPI_ID,
                1 LAYER,
                'maha_140_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2680 KPI_ID,
                1 LAYER,
                'shahad10_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2690 KPI_ID,
                1 LAYER,
                'MissL10_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2700 KPI_ID,
                1 LAYER,
                'Husin10_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2710 KPI_ID,
                1 LAYER,
                'Feda10_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2720 KPI_ID,
                1 LAYER,
                'abrar21_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2730 KPI_ID,
                1 LAYER,
                'feda21_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2740 KPI_ID,
                1 LAYER,
                'roh_n2_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2750 KPI_ID,
                1 LAYER,
                'omseba10_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2760 KPI_ID,
                1 LAYER,
                'marwa10_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2770 KPI_ID,
                1 LAYER,
                'maha_140_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2780 KPI_ID,
                1 LAYER,
                'shahad10_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2790 KPI_ID,
                3 LAYER,
                'CostperorderMissL10_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'MissL10_spend_day,MissL10_orders' KPI_DEPENDENTS,
                'safe_divide(MissL10_spend_day,MissL10_orders)' KPI_CALC,
                'safe_divide(MissL10_spend_day_prevX,MissL10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(MissL10_spend_day_PrevyearsameX,MissL10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2800 KPI_ID,
                3 LAYER,
                'CostperorderHusin10_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'Husin10_spend_day,Husin10_orders' KPI_DEPENDENTS,
                'safe_divide(Husin10_spend_day,Husin10_orders)' KPI_CALC,
                'safe_divide(Husin10_spend_day_prevX,Husin10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Husin10_spend_day_PrevyearsameX,Husin10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2810 KPI_ID,
                3 LAYER,
                'CostperorderFeda10_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'Feda10_spend_day,Feda10_orders' KPI_DEPENDENTS,
                'safe_divide(Feda10_spend_day,Feda10_orders)' KPI_CALC,
                'safe_divide(Feda10_spend_day_prevX,Feda10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Feda10_spend_day_PrevyearsameX,Feda10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2820 KPI_ID,
                3 LAYER,
                'CostperorderAbrar21_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'abrar21_spend_day,abrar21_orders' KPI_DEPENDENTS,
                'safe_divide(abrar21_spend_day,abrar21_orders)' KPI_CALC,
                'safe_divide(abrar21_spend_day_prevX,abrar21_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(abrar21_spend_day_PrevyearsameX,abrar21_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2830 KPI_ID,
                3 LAYER,
                'Costperorderfeda21_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'feda21_spend_day,feda21_orders' KPI_DEPENDENTS,
                'safe_divide(feda21_spend_day,feda21_orders)' KPI_CALC,
                'safe_divide(feda21_spend_day_prevX,feda21_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(feda21_spend_day_PrevyearsameX,feda21_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2840 KPI_ID,
                3 LAYER,
                'CostperorderRohN2_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'roh_n2_spend_day,roh_n2_orders' KPI_DEPENDENTS,
                'safe_divide(roh_n2_spend_day,roh_n2_orders)' KPI_CALC,
                'safe_divide(roh_n2_spend_day_prevX,roh_n2_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(roh_n2_spend_day_PrevyearsameX,roh_n2_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2850 KPI_ID,
                3 LAYER,
                'Costperorderomseba10_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'omseba10_spend_day,omseba10_orders' KPI_DEPENDENTS,
                'safe_divide(omseba10_spend_day,omseba10_orders)' KPI_CALC,
                'safe_divide(omseba10_spend_day_prevX,omseba10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(omseba10_spend_day_PrevyearsameX,omseba10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2860 KPI_ID,
                3 LAYER,
                'Costperordermarwa10_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'marwa10_spend_day,marwa10_orders' KPI_DEPENDENTS,
                'safe_divide(marwa10_spend_day,marwa10_orders)' KPI_CALC,
                'safe_divide(marwa10_spend_day_prevX,marwa10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(marwa10_spend_day_PrevyearsameX,marwa10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2870 KPI_ID,
                3 LAYER,
                'CostperorderMaha140_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'maha_140_spend_day,maha_140_orders' KPI_DEPENDENTS,
                'safe_divide(maha_140_spend_day,maha_140_orders)' KPI_CALC,
                'safe_divide(maha_140_spend_day_prevX,maha_140_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(maha_140_spend_day_PrevyearsameX,maha_140_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2880 KPI_ID,
                3 LAYER,
                'CostperorderShahad10_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'shahad10_spend_day,shahad10_orders' KPI_DEPENDENTS,
                'safe_divide(shahad10_spend_day,shahad10_orders)' KPI_CALC,
                'safe_divide(shahad10_spend_day_prevX,shahad10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(shahad10_spend_day_PrevyearsameX,shahad10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2890 KPI_ID,
                3 LAYER,
                'ATV Customer Acq MissL' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'MissL10_revenue,MissL10_orders' KPI_DEPENDENTS,
                'safe_divide(MissL10_revenue,MissL10_orders)' KPI_CALC,
                'safe_divide(MissL10_revenue_prevX,MissL10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(MissL10_revenue_PrevyearsameX,MissL10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2900 KPI_ID,
                3 LAYER,
                'ATV Customer Acq Husin10' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'Husin10_revenue,Husin10_orders' KPI_DEPENDENTS,
                'safe_divide(Husin10_revenue,Husin10_orders)' KPI_CALC,
                'safe_divide(Husin10_revenue_prevX,Husin10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Husin10_revenue_PrevyearsameX,Husin10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2910 KPI_ID,
                3 LAYER,
                'ATV Customer Acq Feda10' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'Feda10_revenue,Feda10_orders' KPI_DEPENDENTS,
                'safe_divide(Feda10_revenue,Feda10_orders)' KPI_CALC,
                'safe_divide(Feda10_revenue_prevX,Feda10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Feda10_revenue_PrevyearsameX,Feda10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2920 KPI_ID,
                3 LAYER,
                'ATV Customer Acq Abrar21' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'abrar21_revenue,abrar21_orders' KPI_DEPENDENTS,
                'safe_divide(abrar21_revenue,abrar21_orders)' KPI_CALC,
                'safe_divide(abrar21_revenue_prevX,abrar21_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(abrar21_revenue_PrevyearsameX,abrar21_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2930 KPI_ID,
                3 LAYER,
                'ATV Customer Acq Feda21' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'feda21_revenue,feda21_orders' KPI_DEPENDENTS,
                'safe_divide(feda21_revenue,feda21_orders)' KPI_CALC,
                'safe_divide(feda21_revenue_prevX,feda21_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(feda21_revenue_PrevyearsameX,feda21_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2940 KPI_ID,
                3 LAYER,
                'ATV Customer Acq RohN2' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'roh_n2_revenue,roh_n2_orders' KPI_DEPENDENTS,
                'safe_divide(roh_n2_revenue,roh_n2_orders)' KPI_CALC,
                'safe_divide(roh_n2_revenue_prevX,roh_n2_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(roh_n2_revenue_PrevyearsameX,roh_n2_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2950 KPI_ID,
                3 LAYER,
                'ATV Customer Acq Omseba10' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'omseba10_revenue,omseba10_orders' KPI_DEPENDENTS,
                'safe_divide(omseba10_revenue,omseba10_orders)' KPI_CALC,
                'safe_divide(omseba10_revenue_prevX,omseba10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(omseba10_revenue_PrevyearsameX,omseba10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2960 KPI_ID,
                3 LAYER,
                'ATV Customer Acq Marwa10' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'marwa10_revenue,marwa10_orders' KPI_DEPENDENTS,
                'safe_divide(marwa10_revenue,marwa10_orders)' KPI_CALC,
                'safe_divide(marwa10_revenue_prevX,marwa10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(marwa10_revenue_PrevyearsameX,marwa10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2970 KPI_ID,
                3 LAYER,
                'ATV Customer Acq Maha140' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'maha_140_revenue,maha_140_orders' KPI_DEPENDENTS,
                'safe_divide(maha_140_revenue,maha_140_orders)' KPI_CALC,
                'safe_divide(maha_140_revenue_prevX,maha_140_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(maha_140_revenue_PrevyearsameX,maha_140_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2980 KPI_ID,
                3 LAYER,
                'ATV Customer Acq Shahad10' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'shahad10_revenue,shahad10_orders' KPI_DEPENDENTS,
                'safe_divide(shahad10_revenue,shahad10_orders)' KPI_CALC,
                'safe_divide(shahad10_revenue_prevX,shahad10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(shahad10_revenue_PrevyearsameX,shahad10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                2990 KPI_ID,
                3 LAYER,
                'Shareofsales_per MissL10' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'MissL10_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(MissL10_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(MissL10_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(MissL10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3000 KPI_ID,
                3 LAYER,
                'Shareofsales_per Husin10' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'Husin10_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(Husin10_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(Husin10_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(Husin10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3010 KPI_ID,
                3 LAYER,
                'Shareofsales_per Feda10' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'Feda10_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(Feda10_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(Feda10_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(Feda10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3020 KPI_ID,
                3 LAYER,
                'Shareofsales_per Abrar21' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'abrar21_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(abrar21_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(abrar21_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(abrar21_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3030 KPI_ID,
                3 LAYER,
                'Shareofsales_per Feda21' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'feda21_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(feda21_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(feda21_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(feda21_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3040 KPI_ID,
                3 LAYER,
                'Shareofsales_per RohN2' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'roh_n2_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(roh_n2_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(roh_n2_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(roh_n2_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3050 KPI_ID,
                3 LAYER,
                'Shareofsales_per Omseba10' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'omseba10_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(omseba10_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(omseba10_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(omseba10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3060 KPI_ID,
                3 LAYER,
                'Shareofsales_per Marwa10' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'marwa10_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(marwa10_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(marwa10_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(marwa10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3070 KPI_ID,
                3 LAYER,
                'Shareofsales_per Maha140' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'maha_140_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(maha_140_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(maha_140_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(maha_140_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3080 KPI_ID,
                3 LAYER,
                'Shareofsales_per Shahad10' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'shahad10_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(shahad10_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(shahad10_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(shahad10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3090 KPI_ID,
                1 LAYER,
                'Facebook_cust_traffic' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Traffic' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3100 KPI_ID,
                1 LAYER,
                'Instagram_cust_traffic' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Traffic' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3110 KPI_ID,
                1 LAYER,
                'Snapchat_cust_traffic' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Traffic' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3120 KPI_ID,
                1 LAYER,
                'Adwords_cust_traffic' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Traffic' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3130 KPI_ID,
                1 LAYER,
                'Roi_hunter_cust_traffic' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Traffic' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3140 KPI_ID,
                1 LAYER,
                'Facebook_cust_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3150 KPI_ID,
                1 LAYER,
                'Instagram_cust_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3160 KPI_ID,
                1 LAYER,
                'Snapchat_cust_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3170 KPI_ID,
                1 LAYER,
                'Adwords_cust_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3180 KPI_ID,
                1 LAYER,
                'Roi_hunter_cust_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3190 KPI_ID,
                1 LAYER,
                'Facebook_cust_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3200 KPI_ID,
                1 LAYER,
                'Instagram_cust_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3210 KPI_ID,
                1 LAYER,
                'Snapchat_cust_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3220 KPI_ID,
                1 LAYER,
                'Adwords_cust_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3230 KPI_ID,
                1 LAYER,
                'Roi_hunter_cust_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3240 KPI_ID,
                1 LAYER,
                'Facebook_cust_spend' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3250 KPI_ID,
                1 LAYER,
                'google_cust_adspend' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                6250 KPI_ID,
                1 LAYER,
                'google_cust_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Revenue (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3260 KPI_ID,
                1 LAYER,
                'Instagram_cust_spend' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3270 KPI_ID,
                1 LAYER,
                'roi_hunter_cust_spend' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3280 KPI_ID,
                1 LAYER,
                'snapchat_cust_spend' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3290 KPI_ID,
                3 LAYER,
                'Costperorder_Facebook' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'Facebook_cust_spend,Facebook_cust_orders' KPI_DEPENDENTS,
                'safe_divide(Facebook_cust_spend,Facebook_cust_orders)' KPI_CALC,
                'safe_divide(Facebook_cust_spend_prevX,Facebook_cust_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Facebook_cust_spend_PrevyearsameX,Facebook_cust_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3300 KPI_ID,
                3 LAYER,
                'Costperorder_Instagram' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'Instagram_cust_spend,Instagram_cust_orders' KPI_DEPENDENTS,
                'safe_divide(Instagram_cust_spend,Instagram_cust_orders)' KPI_CALC,
                'safe_divide(Instagram_cust_spend_prevX,Instagram_cust_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Instagram_cust_spend_PrevyearsameX,Instagram_cust_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3310 KPI_ID,
                3 LAYER,
                'Costperorder_Snapchat' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'snapchat_cust_spend,Snapchat_cust_orders' KPI_DEPENDENTS,
                'safe_divide(snapchat_cust_spend,Snapchat_cust_orders)' KPI_CALC,
                'safe_divide(snapchat_cust_spend_prevX,Snapchat_cust_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(snapchat_cust_spend_PrevyearsameX,snapchat_cust_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3320 KPI_ID,
                3 LAYER,
                'Costperorder_Adwords' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'google_cust_adspend,Adwords_cust_orders' KPI_DEPENDENTS,
                'safe_divide(google_cust_adspend,Adwords_cust_orders)' KPI_CALC,
                'safe_divide(google_cust_adspend_prevX,Adwords_cust_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(google_cust_adspend_PrevyearsameX,Adwords_cust_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3330 KPI_ID,
                3 LAYER,
                'Costperorder_roi_hunter' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'roi_hunter_cust_spend,Roi_hunter_cust_orders' KPI_DEPENDENTS,
                'safe_divide(roi_hunter_cust_spend,Roi_hunter_cust_orders)' KPI_CALC,
                'safe_divide(roi_hunter_cust_spend_prevX,Roi_hunter_cust_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(roi_hunter_cust_spend_PrevyearsameX,Roi_hunter_cust_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3340 KPI_ID,
                3 LAYER,
                'AOV_Facebook' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'Facebook_cust_revenue,Facebook_cust_orders' KPI_DEPENDENTS,
                'safe_divide(Facebook_cust_revenue,Facebook_cust_orders)' KPI_CALC,
                'safe_divide(Facebook_cust_revenue_prevX,Facebook_cust_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Facebook_cust_revenue_PrevyearsameX,Facebook_cust_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3350 KPI_ID,
                3 LAYER,
                'AOV_Instagram' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'Instagram_cust_revenue,Instagram_cust_orders' KPI_DEPENDENTS,
                'safe_divide(Instagram_cust_revenue,Instagram_cust_orders)' KPI_CALC,
                'safe_divide(Instagram_cust_revenue_prevX,Instagram_cust_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Instagram_cust_revenue_PrevyearsameX,Instagram_cust_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3360 KPI_ID,
                3 LAYER,
                'AOV_Snapchat' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'Snapchat_cust_revenue,Snapchat_cust_orders' KPI_DEPENDENTS,
                'safe_divide(Snapchat_cust_revenue,Snapchat_cust_orders)' KPI_CALC,
                'safe_divide(Snapchat_cust_revenue_prevX,Snapchat_cust_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Snapchat_cust_revenue_PrevyearsameX,Snapchat_cust_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3370 KPI_ID,
                3 LAYER,
                'AOV_Adwords' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'Adwords_cust_revenue,Adwords_cust_orders' KPI_DEPENDENTS,
                'safe_divide(Adwords_cust_revenue,Adwords_cust_orders)' KPI_CALC,
                'safe_divide(Adwords_cust_revenue_prevX,Adwords_cust_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Adwords_cust_revenue_PrevyearsameX,Adwords_cust_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3380 KPI_ID,
                3 LAYER,
                'AOV_roi_hunter' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'Roi_hunter_cust_revenue,Roi_hunter_cust_orders' KPI_DEPENDENTS,
                'safe_divide(Roi_hunter_cust_revenue,Roi_hunter_cust_orders)' KPI_CALC,
                'safe_divide(Roi_hunter_cust_revenue_prevX,Roi_hunter_cust_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Roi_hunter_cust_revenue_PrevyearsameX,Roi_hunter_cust_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3390 KPI_ID,
                3 LAYER,
                'Shareofsales_Facebook' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'Facebook_cust_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(Facebook_cust_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(Facebook_cust_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(Facebook_cust_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3400 KPI_ID,
                3 LAYER,
                'Shareofsales_Instagram' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'Instagram_cust_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(Instagram_cust_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(Instagram_cust_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(Instagram_cust_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3410 KPI_ID,
                3 LAYER,
                'Shareofsales_Snapchat' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'Snapchat_cust_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(Snapchat_cust_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(Snapchat_cust_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(Snapchat_cust_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3420 KPI_ID,
                3 LAYER,
                'Shareofsales_Adwords' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'Adwords_cust_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(Adwords_cust_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(Adwords_cust_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(Adwords_cust_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3430 KPI_ID,
                3 LAYER,
                'Sahreofsales_roi_hunter' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'Roi_hunter_cust_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(Roi_hunter_cust_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(Roi_hunter_cust_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(Roi_hunter_cust_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3440 KPI_ID,
                3 LAYER,
                'ConversionRate_Facebook' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Conversion Rate (%)' KPI_DESCRIPTION,
                'Facebook_cust_orders,Facebook_cust_traffic' KPI_DEPENDENTS,
                'safe_divide(Facebook_cust_orders,Facebook_cust_traffic)' KPI_CALC,
                'safe_divide(Facebook_cust_orders_prevX,Facebook_cust_traffic_prevX)' KPI_CALC_PREVX,
                'safe_divide(Facebook_cust_orders_PrevyearsameX,Facebook_cust_traffic_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3450 KPI_ID,
                3 LAYER,
                'ConversionRate_Instagram' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Conversion Rate (%)' KPI_DESCRIPTION,
                'Instagram_cust_orders,Instagram_cust_traffic' KPI_DEPENDENTS,
                'safe_divide(Instagram_cust_orders,Instagram_cust_traffic)' KPI_CALC,
                'safe_divide(Instagram_cust_orders_prevX,Instagram_cust_traffic_prevX)' KPI_CALC_PREVX,
                'safe_divide(Instagram_cust_orders_PrevyearsameX,Instagram_cust_traffic_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3460 KPI_ID,
                3 LAYER,
                'ConversionRate_Snapchat' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Conversion Rate (%)' KPI_DESCRIPTION,
                'Snapchat_cust_orders,Snapchat_cust_traffic' KPI_DEPENDENTS,
                'safe_divide(Snapchat_cust_orders,Snapchat_cust_traffic)' KPI_CALC,
                'safe_divide(Snapchat_cust_orders_prevX,Snapchat_cust_traffic_prevX)' KPI_CALC_PREVX,
                'safe_divide(Snapchat_cust_orders_PrevyearsameX,Snapchat_cust_traffic_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3470 KPI_ID,
                3 LAYER,
                'ConversionRate_Adwords' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Conversion Rate (%)' KPI_DESCRIPTION,
                'Adwords_cust_orders,Adwords_cust_traffic' KPI_DEPENDENTS,
                'safe_divide(Adwords_cust_orders,Adwords_cust_traffic)' KPI_CALC,
                'safe_divide(Adwords_cust_orders_prevX,Adwords_cust_traffic_prevX)' KPI_CALC_PREVX,
                'safe_divide(Adwords_cust_orders_PrevyearsameX,Adwords_cust_traffic_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3480 KPI_ID,
                3 LAYER,
                'ConversionRate_roi_hunter' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Conversion Rate (%)' KPI_DESCRIPTION,
                'Roi_hunter_cust_orders,Roi_hunter_cust_traffic' KPI_DEPENDENTS,
                'safe_divide(Roi_hunter_cust_orders,Roi_hunter_cust_traffic)' KPI_CALC,
                'safe_divide(Roi_hunter_cust_orders_prevX,Roi_hunter_cust_traffic_prevX)' KPI_CALC_PREVX,
                'safe_divide(Roi_hunter_cust_orders_PrevyearsameX,Roi_hunter_cust_traffic_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3490 KPI_ID,
                1 LAYER,
                'TikTok_paid_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'TikTok Paid Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3500 KPI_ID,
                1 LAYER,
                'TikTok_Unpaid_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'TikTok Unpaid Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3510 KPI_ID,
                1 LAYER,
                'Other_Social_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Other Social Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3520 KPI_ID,
                1 LAYER,
                'Paid_Search_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Paid Search Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3530 KPI_ID,
                1 LAYER,
                'Internal_Referral_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Internal Referral Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3540 KPI_ID,
                1 LAYER,
                'Email_sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Email Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3550 KPI_ID,
                1 LAYER,
                'Organic_Search_Sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Organic Search Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3560 KPI_ID,
                1 LAYER,
                'Referral_Sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Referral Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3570 KPI_ID,
                1 LAYER,
                'Direct_Sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Direct Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3580 KPI_ID,
                1 LAYER,
                'Others_Sessions' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Others Sessions' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3590 KPI_ID,
                3 LAYER,
                'Instagram_Unpaid_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Instagram Unpaid (%)' KPI_DESCRIPTION,
                'Instagram_Unpaid_sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(Instagram_Unpaid_sessions,sessions)' KPI_CALC,
                'safe_divide(Instagram_Unpaid_sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Instagram_Unpaid_sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3600 KPI_ID,
                3 LAYER,
                'TikTok_paid_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'TikTok Paid (%)' KPI_DESCRIPTION,
                'TikTok_paid_sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(TikTok_paid_sessions,sessions)' KPI_CALC,
                'safe_divide(TikTok_paid_sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(TikTok_paid_sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3610 KPI_ID,
                3 LAYER,
                'TikTok_Unpaid_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'TikTok Unpaid (%)' KPI_DESCRIPTION,
                'TikTok_Unpaid_sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(TikTok_Unpaid_sessions,sessions)' KPI_CALC,
                'safe_divide(TikTok_Unpaid_sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(TikTok_Unpaid_sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3620 KPI_ID,
                3 LAYER,
                'Other_Social_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Other Social (%)' KPI_DESCRIPTION,
                'Other_Social_sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(Other_Social_sessions,sessions)' KPI_CALC,
                'safe_divide(Other_Social_sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Other_Social_sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3630 KPI_ID,
                3 LAYER,
                'Paid_Search_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Paid Search (%)' KPI_DESCRIPTION,
                'Paid_Search_sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(Paid_Search_sessions,sessions)' KPI_CALC,
                'safe_divide(Paid_Search_sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Paid_Search_sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3640 KPI_ID,
                3 LAYER,
                'Internal_Referral_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Internal Referral (%)' KPI_DESCRIPTION,
                'Internal_Referral_sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(Internal_Referral_sessions,sessions)' KPI_CALC,
                'safe_divide(Internal_Referral_sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Internal_Referral_sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3650 KPI_ID,
                3 LAYER,
                'Email_sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Email (%)' KPI_DESCRIPTION,
                'Email_sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(Email_sessions,sessions)' KPI_CALC,
                'safe_divide(Email_sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Email_sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3660 KPI_ID,
                3 LAYER,
                'Organic_Search_Sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Organic Search (%)' KPI_DESCRIPTION,
                'Organic_Search_Sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(Organic_Search_Sessions,sessions)' KPI_CALC,
                'safe_divide(Organic_Search_Sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Organic_Search_Sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3670 KPI_ID,
                3 LAYER,
                'Referral_Sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Referral (%)' KPI_DESCRIPTION,
                'Referral_Sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(Referral_Sessions,sessions)' KPI_CALC,
                'safe_divide(Referral_Sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Referral_Sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3680 KPI_ID,
                3 LAYER,
                'Direct_Sessions_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Direct (%)' KPI_DESCRIPTION,
                'Direct_Sessions,sessions' KPI_DEPENDENTS,
                'safe_divide(Direct_Sessions,sessions)' KPI_CALC,
                'safe_divide(Direct_Sessions_prevX,sessions_prevX)' KPI_CALC_PREVX,
                'safe_divide(Direct_Sessions_PrevyearsameX,sessions_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3690 KPI_ID,
                1 LAYER,
                'TikTok_Unpaid_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'TikTok Unpaid Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3700 KPI_ID,
                1 LAYER,
                'Other_Social_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Other Social Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3710 KPI_ID,
                1 LAYER,
                'Paid_Search_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Paid Search Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3720 KPI_ID,
                1 LAYER,
                'Internal_Referral_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Internal Referral Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3730 KPI_ID,
                1 LAYER,
                'Email_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Email Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3740 KPI_ID,
                1 LAYER,
                'Organic_Search_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Organic Search Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3750 KPI_ID,
                1 LAYER,
                'Referral_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Referral Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3760 KPI_ID,
                1 LAYER,
                'Direct_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Direct Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3770 KPI_ID,
                1 LAYER,
                'Others_newusers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Others Newusers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3780 KPI_ID,
                3 LAYER,
                'TikTok_paid_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'TikTok Paid (%)' KPI_DESCRIPTION,
                'TikTok_paid_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(TikTok_paid_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(TikTok_paid_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(TikTok_paid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3790 KPI_ID,
                3 LAYER,
                'TikTok_Unpaid_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'TikTok Unpaid (%)' KPI_DESCRIPTION,
                'TikTok_Unpaid_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(TikTok_Unpaid_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(TikTok_Unpaid_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(TikTok_Unpaid_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3800 KPI_ID,
                3 LAYER,
                'Other_Social_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Other Social (%)' KPI_DESCRIPTION,
                'Other_Social_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(Other_Social_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(Other_Social_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(Other_Social_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3810 KPI_ID,
                3 LAYER,
                'Paid_Search_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Paid Search (%)' KPI_DESCRIPTION,
                'Paid_Search_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(Paid_Search_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(Paid_Search_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(Paid_Search_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3820 KPI_ID,
                3 LAYER,
                'Internal_Referral_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Internal Referral (%)' KPI_DESCRIPTION,
                'Internal_Referral_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(Internal_Referral_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(Internal_Referral_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(Internal_Referral_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3830 KPI_ID,
                3 LAYER,
                'Email_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Email (%)' KPI_DESCRIPTION,
                'Email_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(Email_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(Email_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(Email_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3840 KPI_ID,
                3 LAYER,
                'Organic_Search_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Organic Search (%)' KPI_DESCRIPTION,
                'Organic_Search_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(Organic_Search_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(Organic_Search_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(Organic_Search_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3850 KPI_ID,
                3 LAYER,
                'Referral_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Referral (%)' KPI_DESCRIPTION,
                'Referral_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(Referral_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(Referral_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(Referral_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3860 KPI_ID,
                3 LAYER,
                'Direct_newusers_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Direct (%)' KPI_DESCRIPTION,
                'Direct_newusers,newuserssourcemedium' KPI_DEPENDENTS,
                'safe_divide(Direct_newusers,newuserssourcemedium)' KPI_CALC,
                'safe_divide(Direct_newusers_prevX,newuserssourcemedium_prevX)' KPI_CALC_PREVX,
                'safe_divide(Direct_newusers_PrevyearsameX,newuserssourcemedium_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3870 KPI_ID,
                1 LAYER,
                'Rand10_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3880 KPI_ID,
                1 LAYER,
                'Satha10_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3890 KPI_ID,
                1 LAYER,
                'Wish10_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3900 KPI_ID,
                1 LAYER,
                'Rand10_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3910 KPI_ID,
                1 LAYER,
                'Satha10_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3920 KPI_ID,
                1 LAYER,
                'Wish10_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3930 KPI_ID,
                1 LAYER,
                'Rand10_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3940 KPI_ID,
                1 LAYER,
                'Satha10_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3950 KPI_ID,
                1 LAYER,
                'Wish10_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3960 KPI_ID,
                3 LAYER,
                'CostperorderRand10_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'Rand10_spend_day,Rand10_orders' KPI_DEPENDENTS,
                'safe_divide(Rand10_spend_day,Rand10_orders)' KPI_CALC,
                'safe_divide(Rand10_spend_day_prevX,Rand10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Rand10_spend_day_PrevyearsameX,Rand10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3970 KPI_ID,
                3 LAYER,
                'CostperorderSatha10_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'Satha10_spend_day,Satha10_orders' KPI_DEPENDENTS,
                'safe_divide(Satha10_spend_day,Satha10_orders)' KPI_CALC,
                'safe_divide(Satha10_spend_day_prevX,Satha10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Satha10_spend_day_PrevyearsameX,Satha10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3980 KPI_ID,
                3 LAYER,
                'CostperorderWish10_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'Wish10_spend_day,Wish10_orders' KPI_DEPENDENTS,
                'safe_divide(Wish10_spend_day,Wish10_orders)' KPI_CALC,
                'safe_divide(Wish10_spend_day_prevX,Wish10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Wish10_spend_day_PrevyearsameX,Wish10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                3990 KPI_ID,
                3 LAYER,
                'ATV Customer Acq Rand10' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'Rand10_revenue,Rand10_orders' KPI_DEPENDENTS,
                'safe_divide(Rand10_revenue,Rand10_orders)' KPI_CALC,
                'safe_divide(Rand10_revenue_prevX,Rand10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Rand10_revenue_PrevyearsameX,Rand10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4000 KPI_ID,
                3 LAYER,
                'ATV Customer Acq Satha10' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'Satha10_revenue,Satha10_orders' KPI_DEPENDENTS,
                'safe_divide(Satha10_revenue,Satha10_orders)' KPI_CALC,
                'safe_divide(Satha10_revenue_prevX,Satha10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Satha10_revenue_PrevyearsameX,Satha10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4010 KPI_ID,
                3 LAYER,
                'ATV Customer Acq Wish10' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'Wish10_revenue,Wish10_orders' KPI_DEPENDENTS,
                'safe_divide(Wish10_revenue,Wish10_orders)' KPI_CALC,
                'safe_divide(Wish10_revenue_prevX,Wish10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Wish10_revenue_PrevyearsameX,Wish10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4020 KPI_ID,
                3 LAYER,
                'Shareofsales_per Rand10' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'Rand10_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(Rand10_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(Rand10_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(Rand10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4030 KPI_ID,
                3 LAYER,
                'Shareofsales_per Satha10' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'Satha10_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(Satha10_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(Satha10_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(Satha10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4040 KPI_ID,
                3 LAYER,
                'Shareofsales_per Wish10' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'Wish10_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(Wish10_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(Wish10_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(Wish10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4050 KPI_ID,
                1 LAYER,
                'target' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Target Sales Value (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4060 KPI_ID,
                1 LAYER,
                'total_time_to_confirm' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Time to Confirm (in Days)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4070 KPI_ID,
                1 LAYER,
                'total_time_to_ship' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Time to Ship (in Days)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4080 KPI_ID,
                1 LAYER,
                'total_time_to_deliver' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Time to Deliver (in Days)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4090 KPI_ID,
                1 LAYER,
                'total_time_to_fulfill' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Time to Deliver (in Days)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4100 KPI_ID,
                1 LAYER,
                'orders_created' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Orders Created' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4120 KPI_ID,
                1 LAYER,
                'orders_confirmed' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Orders Confirmed' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4130 KPI_ID,
                1 LAYER,
                'orders_shipped' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Orders Shipped' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4140 KPI_ID,
                1 LAYER,
                'orders_delivered' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Orders Delivered' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4150 KPI_ID,
                3 LAYER,
                'confirmation_rate' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Confirmation Rate (%)' KPI_DESCRIPTION,
                'orders_confirmed,orders_created' KPI_DEPENDENTS,
                'safe_divide(orders_confirmed,orders_created)' KPI_CALC,
                'safe_divide(orders_confirmed_prevX,orders_created_prevX)' KPI_CALC_PREVX,
                'safe_divide(orders_confirmed_PrevyearsameX,orders_created_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4160 KPI_ID,
                3 LAYER,
                'shipment_rate' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Shipment Rate (%)' KPI_DESCRIPTION,
                'orders_shipped,orders_confirmed' KPI_DEPENDENTS,
                'safe_divide(orders_shipped,orders_confirmed)' KPI_CALC,
                'safe_divide(orders_shipped_prevX,orders_confirmed_prevX)' KPI_CALC_PREVX,
                'safe_divide(orders_shipped_PrevyearsameX,orders_confirmed_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4170 KPI_ID,
                3 LAYER,
                'delivery_rate' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Delivery Rate (%)' KPI_DESCRIPTION,
                'orders_delivered,orders_shipped' KPI_DEPENDENTS,
                'safe_divide(orders_delivered,orders_shipped)' KPI_CALC,
                'safe_divide(orders_delivered_prevX,orders_shipped_prevX)' KPI_CALC_PREVX,
                'safe_divide(orders_delivered_PrevyearsameX,orders_shipped_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4180 KPI_ID,
                3 LAYER,
                'fulfilment_rate' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Delivery Rate (%)' KPI_DESCRIPTION,
                'orders_delivered,orders_confirmed' KPI_DEPENDENTS,
                'safe_divide(orders_delivered,orders_confirmed)' KPI_CALC,
                'safe_divide(orders_delivered_prevX,orders_confirmed_prevX)' KPI_CALC_PREVX,
                'safe_divide(orders_delivered_PrevyearsameX,orders_confirmed_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4190 KPI_ID,
                1 LAYER,
                'Yasmine10_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4200 KPI_ID,
                1 LAYER,
                'Yasmine10_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4210 KPI_ID,
                1 LAYER,
                'Yasmine10_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4220 KPI_ID,
                3 LAYER,
                'CostperorderYasmine10_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'Yasmine10_spend_day,Yasmine10_orders' KPI_DEPENDENTS,
                'safe_divide(Yasmine10_spend_day,Yasmine10_orders)' KPI_CALC,
                'safe_divide(Yasmine10_spend_day_prevX,Yasmine10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Yasmine10_spend_day_PrevyearsameX,Yasmine10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4230 KPI_ID,
                3 LAYER,
                'ATV Customer Acq Yasmine10' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'Yasmine10_revenue,Yasmine10_orders' KPI_DEPENDENTS,
                'safe_divide(Yasmine10_revenue,Yasmine10_orders)' KPI_CALC,
                'safe_divide(Yasmine10_revenue_prevX,Yasmine10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Yasmine10_revenue_PrevyearsameX,Yasmine10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4240 KPI_ID,
                3 LAYER,
                'Shareofsales_per Yasmine10' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'Yasmine10_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(Yasmine10_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(Yasmine10_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(Yasmine10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4250 KPI_ID,
                1 LAYER,
                'Fatma10_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4260 KPI_ID,
                1 LAYER,
                'Fatma10_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4270 KPI_ID,
                1 LAYER,
                'Fatma10_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4280 KPI_ID,
                3 LAYER,
                'CostperorderFatma10_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'Fatma10_spend_day,Fatma10_orders' KPI_DEPENDENTS,
                'safe_divide(Fatma10_spend_day,Fatma10_orders)' KPI_CALC,
                'safe_divide(Fatma10_spend_day_prevX,Fatma10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Fatma10_spend_day_PrevyearsameX,Fatma10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4290 KPI_ID,
                3 LAYER,
                'ATV Customer Acq Fatma10' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'Fatma10_revenue,Fatma10_orders' KPI_DEPENDENTS,
                'safe_divide(Fatma10_revenue,Fatma10_orders)' KPI_CALC,
                'safe_divide(Fatma10_revenue_prevX,Fatma10_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Fatma10_revenue_PrevyearsameX,Fatma10_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4300 KPI_ID,
                3 LAYER,
                'Shareofsales_per Fatma10' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'Fatma10_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(Fatma10_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(Fatma10_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(Fatma10_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4310 KPI_ID,
                1 LAYER,
                'target_spend' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Target Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4320 KPI_ID,
                1 LAYER,
                'facebook_target_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Target Spend (in SAR) - Facebook & Instagram' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4330 KPI_ID,
                1 LAYER,
                'adwords_target_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Target Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4340 KPI_ID,
                1 LAYER,
                'snapchat_target_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Target Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4350 KPI_ID,
                1 LAYER,
                'on_time_shipments' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number of Shipments Delivered On Time' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4360 KPI_ID,
                1 LAYER,
                'delayed_shipments' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number of Shipments Delayed' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4370 KPI_ID,
                1 LAYER,
                'refused_shipments' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number of Shipments Refused (by Customers)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4380 KPI_ID,
                1 LAYER,
                'total_attempts' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Delivery Attempts' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4390 KPI_ID,
                1 LAYER,
                'total_tickets_created' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number of Tickets Created' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4400 KPI_ID,
                1 LAYER,
                'facebook_tickets' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Facebook' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4410 KPI_ID,
                1 LAYER,
                'chat_tickets' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Chat' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4420 KPI_ID,
                1 LAYER,
                'email_tickets' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Email' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4430 KPI_ID,
                1 LAYER,
                'twitter_tickets' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Twitter' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4440 KPI_ID,
                1 LAYER,
                'web_tickets' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Web' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4450 KPI_ID,
                1 LAYER,
                'other_cases' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Others' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4460 KPI_ID,
                1 LAYER,
                'total_days_to_close' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Time Taken To Close' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4470 KPI_ID,
                1 LAYER,
                'new_tickets' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'New' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4480 KPI_ID,
                1 LAYER,
                'open_tickets' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Open' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4490 KPI_ID,
                1 LAYER,
                'closed_tickets' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Closed' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4500 KPI_ID,
                1 LAYER,
                'pending_tickets' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Pending' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4510 KPI_ID,
                1 LAYER,
                'solved_tickets' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Solved' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4520 KPI_ID,
                1 LAYER,
                'deleted_tickets' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Deleted' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4530 KPI_ID,
                1 LAYER,
                'general_query' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'General Query' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4540 KPI_ID,
                1 LAYER,
                'return' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Return/Refund' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4550 KPI_ID,
                1 LAYER,
                'exchange' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Exchange' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4560 KPI_ID,
                1 LAYER,
                'order_delay' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Delay in Delivery' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4570 KPI_ID,
                1 LAYER,
                'wrong_order' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Wrong Order' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4580 KPI_ID,
                1 LAYER,
                'manufacturing' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Manufacturing' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4590 KPI_ID,
                1 LAYER,
                'website_issue' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Website Problems' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4600 KPI_ID,
                3 LAYER,
                'total_days_to_close_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Time Taken (in Days) to Close' KPI_DESCRIPTION,
                'total_days_to_close,closed_tickets' KPI_DEPENDENTS,
                'safe_divide(total_days_to_close,closed_tickets)' KPI_CALC,
                'safe_divide(total_days_to_close_prevX,closed_tickets_prevX)' KPI_CALC_PREVX,
                'safe_divide(total_days_to_close_PrevyearsameX,closed_tickets_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4610 KPI_ID,
                3 LAYER,
                'total_time_to_confirm_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Time Taken (in Days) to Confirm (by CRM)' KPI_DESCRIPTION,
                'total_time_to_confirm,orders_confirmed' KPI_DEPENDENTS,
                'safe_divide(total_time_to_confirm,orders_confirmed)' KPI_CALC,
                'safe_divide(total_time_to_confirm_prevX,orders_confirmed_prevX)' KPI_CALC_PREVX,
                'safe_divide(total_time_to_confirm_PrevyearsameX,orders_confirmed_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4620 KPI_ID,
                3 LAYER,
                'total_time_to_ship_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Time Taken (in Days) to Ship (After Confirmation)' KPI_DESCRIPTION,
                'total_time_to_ship,orders_shipped' KPI_DEPENDENTS,
                'safe_divide(total_time_to_ship,orders_shipped)' KPI_CALC,
                'safe_divide(total_time_to_ship_prevX,orders_shipped_prevX)' KPI_CALC_PREVX,
                'safe_divide(total_time_to_ship_PrevyearsameX,orders_shipped_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4630 KPI_ID,
                3 LAYER,
                'total_time_to_deliver_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Time Taken (in Days) to Deliver (After Shippment)' KPI_DESCRIPTION,
                'total_time_to_deliver,orders_delivered' KPI_DEPENDENTS,
                'safe_divide(total_time_to_deliver,orders_delivered)' KPI_CALC,
                'safe_divide(total_time_to_deliver_prevX,orders_delivered_prevX)' KPI_CALC_PREVX,
                'safe_divide(total_time_to_deliver_PrevyearsameX,orders_delivered_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4640 KPI_ID,
                3 LAYER,
                'total_time_to_fulfill_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Time Taken (in Days) to Deliver (After Confirmation)' KPI_DESCRIPTION,
                'total_time_to_fulfill,orders_delivered' KPI_DEPENDENTS,
                'safe_divide(total_time_to_fulfill,orders_delivered)' KPI_CALC,
                'safe_divide(total_time_to_fulfill_prevX,orders_delivered_prevX)' KPI_CALC_PREVX,
                'safe_divide(total_time_to_fulfill_PrevyearsameX,orders_delivered_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4650 KPI_ID,
                3 LAYER,
                'total_attempts_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Delivery Attempts' KPI_DESCRIPTION,
                'total_attempts,shipments' KPI_DEPENDENTS,
                'safe_divide(total_attempts,shipments)' KPI_CALC,
                'safe_divide(total_attempts_prevX,shipments_prevX)' KPI_CALC_PREVX,
                'safe_divide(total_attempts_PrevyearsameX,shipments_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4660 KPI_ID,
                1 LAYER,
                'shipments' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Shipments' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4670 KPI_ID,
                1 LAYER,
                'missl2u_spend_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4680 KPI_ID,
                1 LAYER,
                'missl2u_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4690 KPI_ID,
                1 LAYER,
                'missl2u_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4700 KPI_ID,
                3 LAYER,
                'Costperordermissl2u_per' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'missl2u_spend_day,missl2u_orders' KPI_DEPENDENTS,
                'safe_divide(missl2u_spend_day,missl2u_orders)' KPI_CALC,
                'safe_divide(missl2u_spend_day_prevX,missl2u_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(missl2u_spend_day_PrevyearsameX,missl2u_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4710 KPI_ID,
                3 LAYER,
                'ATV Customer Acq missl2u' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'missl2u_revenue,missl2u_orders' KPI_DEPENDENTS,
                'safe_divide(missl2u_revenue,missl2u_orders)' KPI_CALC,
                'safe_divide(missl2u_revenue_prevX,missl2u_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(missl2u_revenue_PrevyearsameX,missl2u_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4720 KPI_ID,
                3 LAYER,
                'Shareofsales_per missl2u' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'missl2u_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(missl2u_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(missl2u_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(missl2u_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4730 KPI_ID,
                1 LAYER,
                'Existing_Customers' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Returning Customers' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4740 KPI_ID,
                3 LAYER,
                'CAC Total' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Customer Acquisition Cost' KPI_DESCRIPTION,
                'snapchat_cust_spend,spend,MissL10_spend_day,Husin10_spend_day,Feda10_spend_day,abrar21_spend_day,feda21_spend_day,roh_n2_spend_day,omseba10_spend_day,marwa10_spend_day,maha_140_spend_day,shahad10_spend_day,Rand10_spend_day,Satha10_spend_day,Wish10_spend_day,Yasmine10_spend_day,Fatma10_spend_day,missl2u_spend_day,New_customers' KPI_DEPENDENTS,
                'safe_divide((snapchat_cust_spend+spend+MissL10_spend_day+Husin10_spend_day+Feda10_spend_day+abrar21_spend_day+feda21_spend_day+roh_n2_spend_day+omseba10_spend_day+marwa10_spend_day+maha_140_spend_day+shahad10_spend_day+Rand10_spend_day+Satha10_spend_day+Wish10_spend_day+Yasmine10_spend_day+Fatma10_spend_day+missl2u_spend_day),New_customers)' KPI_CALC,
                'safe_divide((snapchat_cust_spend_prevX+spend_prevX+MissL10_spend_day_prevX+Husin10_spend_day_prevX+Feda10_spend_day_prevX+abrar21_spend_day_prevX+feda21_spend_day_prevX+roh_n2_spend_day_prevX+omseba10_spend_day_prevX+marwa10_spend_day_prevX+maha_140_spend_day_prevX+shahad10_spend_day_prevX+Rand10_spend_day_prevX+Satha10_spend_day_prevX+Wish10_spend_day_prevX+Yasmine10_spend_day_prevX+Fatma10_spend_day_prevX+missl2u_spend_day),New_customers)' KPI_CALC_PREVX,
                'safe_divide((snapchat_cust_spend_PrevyearsameX+spend_PrevyearsameX+MissL10_spend_day_PrevyearsameX+Husin10_spend_day_PrevyearsameX+Feda10_spend_day_PrevyearsameX+abrar21_spend_day_PrevyearsameX+feda21_spend_day_PrevyearsameX+roh_n2_spend_day_PrevyearsameX+omseba10_spend_day_PrevyearsameX+marwa10_spend_day_PrevyearsameX+maha_140_spend_day_PrevyearsameX+shahad10_spend_day_PrevyearsameX+Rand10_spend_day_PrevyearsameX+Satha10_spend_day_PrevyearsameX+Wish10_spend_day_PrevyearsameX+Yasmine10_spend_day_PrevyearsameX+Fatma10_spend_day_PrevyearsameX+missl2u_spend_day),New_customers)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4750 KPI_ID,
                1 LAYER,
                'Youtube_cust_traffic' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Traffic' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4760 KPI_ID,
                1 LAYER,
                'Tiktok_cust_traffic' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Traffic' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4770 KPI_ID,
                1 LAYER,
                'Youtube_cust_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4780 KPI_ID,
                1 LAYER,
                'Tiktok_cust_orders' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number Of Successful Orders' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4790 KPI_ID,
                1 LAYER,
                'Tiktok_cust_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4800 KPI_ID,
                1 LAYER,
                'Youtube_cust_revenue' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Sales Value (With VAT in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4810 KPI_ID,
                1 LAYER,
                'spend_per_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4820 KPI_ID,
                3 LAYER,
                'Costperorder_youtube' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'spend_per_day,Youtube_cust_orders' KPI_DEPENDENTS,
                'safe_divide(spend_per_day,Youtube_cust_orders)' KPI_CALC,
                'safe_divide(spend_per_day_prevX,Youtube_cust_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(spend_per_day_PrevyearsameX,Youtube_cust_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4830 KPI_ID,
                3 LAYER,
                'AOV_youtube' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'Youtube_cust_revenue,Youtube_cust_orders' KPI_DEPENDENTS,
                'safe_divide(Youtube_cust_revenue,Youtube_cust_orders)' KPI_CALC,
                'safe_divide(Youtube_cust_revenue_prevX,Youtube_cust_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Youtube_cust_revenue_PrevyearsameX,Youtube_cust_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4840 KPI_ID,
                3 LAYER,
                'AOV_tiktok' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Average Order Value (in SAR)' KPI_DESCRIPTION,
                'Tiktok_cust_revenue,Tiktok_cust_orders' KPI_DEPENDENTS,
                'safe_divide(Tiktok_cust_revenue,Tiktok_cust_orders)' KPI_CALC,
                'safe_divide(Tiktok_cust_revenue_prevX,Tiktok_cust_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(Tiktok_cust_revenue_PrevyearsameX,Tiktok_cust_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4850 KPI_ID,
                3 LAYER,
                'Shareofsales_youtube' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'Youtube_cust_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(Youtube_cust_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(Youtube_cust_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(Youtube_cust_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4860 KPI_ID,
                3 LAYER,
                'Shareofsales_tiktok' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Share Of Sales (%)' KPI_DESCRIPTION,
                'Tiktok_cust_revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(Tiktok_cust_revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(Tiktok_cust_revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(Tiktok_cust_revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4870 KPI_ID,
                3 LAYER,
                'ConversionRate_youtube' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Conversion Rate (%)' KPI_DESCRIPTION,
                'Youtube_cust_orders,Youtube_cust_traffic' KPI_DEPENDENTS,
                'safe_divide(Youtube_cust_orders,Youtube_cust_traffic)' KPI_CALC,
                'safe_divide(Youtube_cust_orders_prevX,Youtube_cust_traffic_prevX)' KPI_CALC_PREVX,
                'safe_divide(Youtube_cust_orders_PrevyearsameX,Youtube_cust_traffic_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4880 KPI_ID,
                3 LAYER,
                'ConversionRate_tiktok' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'Conversion Rate (%)' KPI_DESCRIPTION,
                'Tiktok_cust_orders,Tiktok_cust_traffic' KPI_DEPENDENTS,
                'safe_divide(Tiktok_cust_orders,Tiktok_cust_traffic)' KPI_CALC,
                'safe_divide(Tiktok_cust_orders_prevX,Tiktok_cust_traffic_prevX)' KPI_CALC_PREVX,
                'safe_divide(Tiktok_cust_orders_PrevyearsameX,Tiktok_cust_traffic_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4890 KPI_ID,
                1 LAYER,
                'sku_more_than_six' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number of SKUs ageing > 6 months' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4900 KPI_ID,
                1 LAYER,
                'sku_more_than_three' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number of SKUs ageing 3-6 months' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4910 KPI_ID,
                1 LAYER,
                'sku_less_than_three' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number of SKUs ageing < 3 months' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4920 KPI_ID,
                1 LAYER,
                'num_of_skus' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number of SKUs successfully sold' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4930 KPI_ID,
                1 LAYER,
                'skus_80_20' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number of SKUs contributing to 80% of Sales' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4940 KPI_ID,
                3 LAYER,
                'skus_80_20_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                '% of SKUs contributing to 80% of Sales' KPI_DESCRIPTION,
                'skus_80_20,num_of_skus' KPI_DEPENDENTS,
                'safe_divide(skus_80_20,num_of_skus)' KPI_CALC,
                'safe_divide(skus_80_20_prevX,num_of_skus_prevX)' KPI_CALC_PREVX,
                'safe_divide(skus_80_20_PrevyearsameX,num_of_skus_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4950 KPI_ID,
                1 LAYER,
                'no_launched' KPI_VARIABLE,
                'Integer' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Number of SKUs Activated' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4960 KPI_ID,
                3 LAYER,
                'New_customer_Revenue_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                '% of Sales happened due to New customers' KPI_DESCRIPTION,
                'New_customer_Revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(New_customer_Revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(New_customer_Revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(New_customer_Revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4970 KPI_ID,
                3 LAYER,
                'Existing_customer_Revenue_per' KPI_VARIABLE,
                'Percentage' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                '% of Sales happened due to Existing customers' KPI_DESCRIPTION,
                'Existing_customer_Revenue,total_item_price_auth' KPI_DEPENDENTS,
                'safe_divide(Existing_customer_Revenue,total_item_price_auth)' KPI_CALC,
                'safe_divide(Existing_customer_Revenue_prevX,total_item_price_auth_prevX)' KPI_CALC_PREVX,
                'safe_divide(Existing_customer_Revenue_PrevyearsameX,total_item_price_auth_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4980 KPI_ID,
                1 LAYER,
                'tiktok_target_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Target Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                4990 KPI_ID,
                1 LAYER,
                'tiktok_cust_spend' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Total Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                5000 KPI_ID,
                3 LAYER,
                'Costperorder_Tiktok' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'CPO (in SAR)' KPI_DESCRIPTION,
                'tiktok_cust_spend,Tiktok_cust_orders' KPI_DEPENDENTS,
                'safe_divide(tiktok_cust_spend,Tiktok_cust_orders)' KPI_CALC,
                'safe_divide(tiktok_cust_spend_prevX,Tiktok_cust_orders_prevX)' KPI_CALC_PREVX,
                'safe_divide(tiktok_cust_spend_PrevyearsameX,Tiktok_cust_orders_PrevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                5010 KPI_ID,
                3 LAYER,
                'ROAS_1' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'ROAS' KPI_DESCRIPTION,
                'NetRevenue,spend' KPI_DEPENDENTS,
                'safe_divide(NetRevenue,spend)' KPI_CALC,
                'safe_divide(NetRevenue_prevX,spend_prevX)' KPI_CALC_PREVX,
                'safe_divide(NetRevenue_prevyearsameX,spend_prevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                5020 KPI_ID,
                1 LAYER,
                'youtube_target_day' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIs' KPI_SQL,
                'Target Spend (in SAR)' KPI_DESCRIPTION,
                '' KPI_DEPENDENTS,
                '' KPI_CALC,
                '' KPI_CALC_PREVX,
                '' KPI_CALC_PREVYEARSAMEX
            
                union all
            
        
                select
                5030 KPI_ID,
                3 LAYER,
                'ROAS_2' KPI_VARIABLE,
                'Decimal' KPI_FORMAT,
                'fKPIsCalc' KPI_SQL,
                'ROAS' KPI_DESCRIPTION,
                'NetRevenue,spend' KPI_DEPENDENTS,
                'safe_divide(NetRevenue,spend)' KPI_CALC,
                'safe_divide(NetRevenue_prevX,spend_prevX)' KPI_CALC_PREVX,
                'safe_divide(NetRevenue_prevyearsameX,spend_prevyearsameX)' KPI_CALC_PREVYEARSAMEX
            
        
)