#' Generate data frame containing new customer data for entire year
#'
#'
#'\code{fetch_new_NCtraining_data} returns a data frame consisting of all new customers from the supplied year.
#'
#' \strong{WARNING: THIS FUNCTION CAN TAKE >90 MINUTES TO COMPLETE QUERY}.
#'
#' This function will produce a data frame containing all unique customers of a
#' given year, and >85 variables related to those customers. The query as
#' written is restricted to new customers and does not generate data beyond
#' information regarding their first purchase.
#'
#' @param year is the year of interest. Must be in the format YYYY. The function
#'   will convert to proper data type.
#'
#' @param conn is the name of a valid data base connection.
#'
#' @return This function returns a single data frame.
#'
#' @examples
#' \dontrun{
#'   con = dbConnect(odbc::odbc(), 'EDW_BA_PROD', uid = 'danwal1', pwd = 'XXXXXXXX')
#' }
#'
#'  df = fetch_new_NCtraining_data(2018, conn = con)
#'
#'
#'

fetch_new_NCtraining_data = function(year, conn){

  confirm = readline(prompt = "WARNING! Query takes >30 minutes to build from scratch! Continue? Y/n...")

  if(confirm == "Y"){


    if(year< 2008){
      print("Entered Year out of range. Please enter a year >= 2008.")
    }else{
      year_i = as.character(year)
      dat_q = paste("SELECT /*+ parallel(8) */
                    SALES.*,
                    LTV.prob_alive,
                    ROUND(LTV.GROSS_MARGIN_AVERAGE_ORDER_DT,2) as GROSS_MARGIN_AVERAGE_ORDER_DT,
                    ROUND(LTV.EXP_TRANS_1YR_LTV,2) as EXP_TRANS_1YR_LTV,
                    ROUND(LTV.EXP_TRANS_3YR_LTV,2) as EXP_TRANS_3YR_LTV,
                    ROUND(LTV.EXP_TRANS_5YR_LTV,2) as EXP_TRANS_5YR_LTV,
                    LTV_1YR.prob_alive as prob_alive_1YR,
                    ROUND(LTV_1YR.GROSS_MARGIN_AVERAGE_ORDER_DT,2) as GROSS_MARGIN_AVERAGE_ORDER_DT_1YR,
                    ROUND(LTV_1YR.EXP_TRANS_1YR_LTV,2) as EXP_TRANS_1YR_LTV_1YR,
                    ROUND(LTV_1YR.EXP_TRANS_3YR_LTV,2) as EXP_TRANS_3YR_LTV_1YR,
                    ROUND(LTV_1YR.EXP_TRANS_5YR_LTV,2) as EXP_TRANS_5YR_LTV_1YR
                    FROM (
                    SELECT /*+ PARALLEL(16) */
                    s.customer_id,
                    /******** VARIABLE SFOR CLUSTERING *********/
                    --RAW FINANCIALS
                    --ROUND(sum(s.net_revenue),4) as net_revenue, --RETURNS BUILT IN, WANT PLACED ORDER METRICS
                    --ROUND(sum(s.gross_margin),4) as gross_margin, --RETURNS BUILT IN, WANT PLACED ORDER METRICS
                    --ROUND(sum(s.GROSS_MARGIN_PRODUCT),4) as gross_margin_product, --RETURNS BUILT IN, WANT PLACED ORDER METRICS
                    trunc(min(s.order_date_time),'IW')+7 as week_ending,
                    --CUSTOMER CLASSIFICATION METRICS
                    max(s.RETURN_ORDER_FLAG) as RETURN_ORDER_FLAG,
                    max(t.cust_class_chnnl_phone_flag) as cust_class_chnnl_phone_flag,
                    max(t.cust_class_chnnl_website_flag) as cust_class_chnnl_website_flag,
                    --max(t.cust_class_chnnl_omni_flag) as cust_class_chnnl_omni_flag, --NOT NEEDED FOR NC BECAUSE THEY CAN ONLY ORDER IN ONE CHANNEL
                    --max(case when upper(s.sales_SUBCHANNEL_NAME) = 'CALL CENTER' then 1 else 0 end) as phone_cust, --SHOULD BE REDUNDANT WITH cust_class_chnnl_phone_flag
                    --max(case when upper(s.sales_SUBCHANNEL_NAME) = 'JTV.COM' then 1 else 0 end) as website_cust, --SHOULD BE REDUNDANT WITH cust_class_chnnl_website_flag
                    --max(case when upper(s.sales_SUBCHANNEL_NAME) = 'AOS' then 1 else 0 end) as AOS_cust, --NOT NEEDED FOR NC BECAUSE THEY CANT PLACE FIRST ORDER ON AOS
                    max(case when upper(s.BROADCAST_INFLUENCED) = 'INFLUENCED' then 1 else 0 end) as BCAST_INFLUENCED_CUST,
                    max(case when upper(s.BROADCAST_INFLUENCED) = 'ANYTIME' then 1 else 0 end) as ANYTIME_CUST,
                    --RAW FINANCIALS - CAN BE USED FOR SUMMARY DATA AS WELL
                    ROUND(sum(s.CANCEL_RATE),4) as NUM_ORDERS,
                    ROUND(sum(s.GROSS_SALES),4) as GROSS_PRODUCT_SALES,
                    ROUND(SUM(s.PRODUCT_QUANTITY*s.cancel_rate),4) as PRODUCT_QTY,
                    ROUND(SUM(S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate),2) AS EXTENDED_PRODUCT_RETAIL_PRICE,
                    ROUND(SUM(S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate),2) as EXTENDED_PRODUCT_DISCOUNT, --DISCOUNT RATE => EXTENDED_PRODUCT_DISCOUNT / EXTENDED_PRODUCT_RETAIL_PRICE       SOLD UNIT PP => (EXTENDED_PRODUCT_RETAIL_PRICE + EXTENDED_PRODUCT_DISCOUNT) / PRODUCT_QTY
                    ROUND(SUM(S.PRODUCT_UNIT_COST*S.PRODUCT_QUANTITY*s.cancel_rate),2) as EXTENDED_PRODUCT_COST,--IMU => 1-(EXTENDED_PRODUCT_COST / EXTENDED_PRODUCT_RETAIL_PRICE)
                    ROUND(SUM(s.EXTENDED_SHIPPING_PRICE),4) as EXTENDED_SHIPPING_PRICE,
                    ROUND(SUM(s.EXTENDED_SHIPPING_DISCOUNT),4) AS EXTENDED_SHIPPING_DISCOUNT, --SHIPPING DISCOUNT RATE => EXTENDED_SHIPPING_DISCOUNT / EXTENDED_SHIPPING_PRICE
                    --CALCULATED FINANCIAL METRICS PER CUST
                    ROUND(SUM(S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate)+SUM(S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate)-SUM(S.PRODUCT_UNIT_COST*S.PRODUCT_QUANTITY*s.cancel_rate),2) AS PRODUCT_GROSS_MARGIN,
                    ROUND(1-(SUM(S.PRODUCT_UNIT_COST*S.PRODUCT_QUANTITY*s.cancel_rate)/SUM(S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate)),4) as IMU,
                    --RATE METRICS PER CUST
                    CASE WHEN SUM(S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) > 0 then ABS(ROUND(SUM(S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate)/SUM(S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate),4)) else null end as PRODUCT_DISCOUNT_RATE,
                    ROUND((SUM(S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate)+SUM(S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate))/SUM(S.PRODUCT_QUANTITY*s.cancel_rate),2) as PRODUCT_UNIT_PP,
                    CASE WHEN SUM(s.EXTENDED_SHIPPING_PRICE) > 0 then ABS(ROUND(SUM(s.EXTENDED_SHIPPING_DISCOUNT)/SUM(s.EXTENDED_SHIPPING_PRICE),4)) else null end as SHIPPING_DISCOUNT_RATE,
                    --PAYMENT TYPE
                    max(case when upper(s.PAYMENT_METHOD_SUBTYPE_CODE) = 'PA' then 1 else 0 end) as PA_CUST,
                    --DEPARTMENT METRICS
                    count(distinct s.DEPARTMENT_CURRENT) as num_depts,
                    max(CASE WHEN upper(s.department_current) = 'BELLA LUCE' then 1 else 0 end) as BELLA_CUST,
                    max(CASE WHEN upper(s.department_current) = 'BELLA LUCE DESIGNERS' then 1 else 0 end) as BELLA_DESIGNER_CUST,
                    max(CASE WHEN upper(s.department_current) = 'COSTUME JEWELRY' then 1 else 0 end) as COSTUME_CUST,
                    max(CASE WHEN upper(s.department_current) = 'COLOR GOLD' then 1 else 0 end) as COLOR_GOLD_CUST,
                    max(CASE WHEN upper(s.department_current) = 'GOLD' then 1 else 0 end) as GOLD_CUST,
                    max(CASE WHEN upper(s.department_current) = 'PEARLS' then 1 else 0 end) as PEARLS_CUST,
                    max(CASE WHEN upper(s.department_current) = 'SILVER' then 1 else 0 end) as SILVER_CUST,
                    max(CASE WHEN upper(s.department_current) = 'COLOR SILVER' then 1 else 0 end) as COLOR_SILVER_CUST,
                    max(CASE WHEN upper(s.department_current) = 'COLOR SILVER BRANDS' then 1 else 0 end) as COLOR_SILVER_BRANDS_CUST,
                    max(CASE WHEN upper(s.department_current) = 'DIAMOND GOLD' then 1 else 0 end) as DIAMOND_GOLD_CUST,
                    max(CASE WHEN upper(s.department_current) = 'DIAMOND SILVER' then 1 else 0 end) as DIAMOND_SILVER_CUST,
                    max(CASE WHEN upper(s.department_current) = 'DIAMOND SYNTHETIC' then 1 else 0 end) as DIAMOND_SYNTH_CUST,
                    max(CASE WHEN upper(s.department_current) NOT IN ('BELLA LUCE','BELLA LUCE DESIGNERS','COSTUME JEWELRY','COLOR GOLD','GOLD','PEARLS','SILVER','COLOR SILVER','COLOR SILVER BRANDS','DIAMOND GOLD','DIAMOND SILVER','DIAMOND SYNTHETIC') then 1 else 0 end) as OTHER_DEPT_CUST,

                    --FOR CLUSTER SUMMARY STATS
                    --PRODUCT SALES
                    sum(CASE WHEN upper(s.department_current) = 'BELLA LUCE' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as BELLA_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'BELLA LUCE DESIGNERS' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as BELLA_DESIGNER_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'COSTUME JEWELRY' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COSTUME_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR GOLD' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COLOR_GOLD_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'GOLD' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as GOLD_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'PEARLS' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as PEARLS_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'SILVER' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as SILVER_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR SILVER' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COLOR_SILVER_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR SILVER BRANDS' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COLOR_SILVER_BRANDS_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND GOLD' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as DIAMOND_GOLD_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND SILVER' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as DIAMOND_SILVER_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND SYNTHETIC' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as DIAMOND_SYNTH_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) NOT IN ('BELLA LUCE','BELLA LUCE DESIGNERS','COSTUME JEWELRY','COLOR GOLD','GOLD','PEARLS','SILVER','COLOR SILVER','COLOR SILVER BRANDS','DIAMOND GOLD','DIAMOND SILVER','DIAMOND SYNTHETIC') then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as OTHER_DEPT_PROD_SALES,
                    --PRODUCT DISCOUNTS => CALCULATE DISC RATE => ABS(PRODUCT DISCOUNTS / PRODUCT SALES)
                    sum(CASE WHEN upper(s.department_current) = 'BELLA LUCE' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as BELLA_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'BELLA LUCE DESIGNERS' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as BELLA_DESIGNER_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'COSTUME JEWELRY' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as COSTUME_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR GOLD' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as COLOR_GOLD_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'GOLD' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as GOLD_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'PEARLS' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as PEARLS_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'SILVER' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as SILVER_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR SILVER' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as COLOR_SILVER_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR SILVER BRANDS' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as COLOR_SILVER_BRANDS_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND GOLD' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as DIAMOND_GOLD_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND SILVER' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as DIAMOND_SILVER_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND SYNTHETIC' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as DIAMOND_SYNTH_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) NOT IN ('BELLA LUCE','BELLA LUCE DESIGNERS','COSTUME JEWELRY','COLOR GOLD','GOLD','PEARLS','SILVER','COLOR SILVER','COLOR SILVER BRANDS','DIAMOND GOLD','DIAMOND SILVER','DIAMOND SYNTHETIC') then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as OTHER_DEPT_PROD_DISC,
                    --PRODUCT QTY => CALCULATE SOLD UNIT PP => (PRODUCT SALES + PRODUCT DISCOUNTS) / PRODUCT QTY
                    sum(CASE WHEN upper(s.department_current) = 'BELLA LUCE' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as BELLA_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'BELLA LUCE DESIGNERS' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as BELLA_DESIGNER_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'COSTUME JEWELRY' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COSTUME_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR GOLD' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COLOR_GOLD_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'GOLD' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as GOLD_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'PEARLS' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as PEARLS_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'SILVER' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as SILVER_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR SILVER' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COLOR_SILVER_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR SILVER BRANDS' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COLOR_SILVER_BRANDS_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND GOLD' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as DIAMOND_GOLD_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND SILVER' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as DIAMOND_SILVER_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND SYNTHETIC' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as DIAMOND_SYNTH_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) NOT IN ('BELLA LUCE','BELLA LUCE DESIGNERS','COSTUME JEWELRY','COLOR GOLD','GOLD','PEARLS','SILVER','COLOR SILVER','COLOR SILVER BRANDS','DIAMOND GOLD','DIAMOND SILVER','DIAMOND SYNTHETIC') then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as OTHER_DEPT_PROD_QTY

                    from BA_SCHEMA.SALES s
                    left outer join BA_SCHEMA.CUSTOMER_CLASSIFICATION_STATUS t
                    on nvl(s.order_reference_number,s.ORDER_NUMBER) = t.order_reference_number
                    where s.order_date_time >= to_date('01-jan-2016')-364
                    AND s.order_date_time < to_date('01-jan-2016')
                    AND upper(s.sales_SUBCHANNEL_NAME) NOT IN ('AUCTION', 'EMPLOYEE','CUSTOMER SERVICE','UNKNOWN')
                    AND t.ORD_CUST_CLASS_STATUS_NEW_FLAG = 1
                    AND S.PRODUCT_QUANTITY*s.cancel_rate > 0
                    group by s.customer_id
                    ) SALES
                    left outer join BA_SCHEMA.CUSTOMER_LIFETIME_VALUE LTV
                    on SALES.customer_id = LTV.customer_id
                    and SALES.week_ending = LTV.week_of
                    left outer join BA_SCHEMA.CUSTOMER_LIFETIME_VALUE LTV_1YR
                    on SALES.customer_id = LTV_1YR.customer_id
                    and SALES.week_ending+(7*52) = LTV_1YR.week_of
                    ;


                    /*----------------------------------------------------------------------------*/
                    /* DIAGNOSTIC SCRIPTS */

                    select t.* from DANWAL1.TEST_NCCLUSTERS_TRAIN1 t where rownum<= 10 order by 2,1 ;

                    select distinct(t.week_ending) from DANWAL1.TEST_NCCLUSTERS_TRAIN1 t ORDER BY 1;

                    select count(*) from DANWAL1.TEST_NCCLUSTERS_TRAIN1 t;

                    select t.week_ending,
                    count(*)
                    from DANWAL1.TEST_NCCLUSTERS_TRAIN1 t
                    group by t.week_ending
                    ORDER BY 1;
                    /*----------------------------------------------------------------------------*/

                    drop table TEST_NCCLUSTERS_TRAIN2;
                    /*INITIAL BUILD TOOK 98 MINS
                    229,397 records total*/
                    create table TEST_NCCLUSTERS_TRAIN2 AS
                    SELECT /*+ parallel(8) */
                    SALES.*,
                    LTV.prob_alive,
                    ROUND(LTV.GROSS_MARGIN_AVERAGE_ORDER_DT,2) as GROSS_MARGIN_AVERAGE_ORDER_DT,
                    ROUND(LTV.EXP_TRANS_1YR_LTV,2) as EXP_TRANS_1YR_LTV,
                    ROUND(LTV.EXP_TRANS_3YR_LTV,2) as EXP_TRANS_3YR_LTV,
                    ROUND(LTV.EXP_TRANS_5YR_LTV,2) as EXP_TRANS_5YR_LTV,
                    LTV_1YR.prob_alive as prob_alive_1YR,
                    ROUND(LTV_1YR.GROSS_MARGIN_AVERAGE_ORDER_DT,2) as GROSS_MARGIN_AVERAGE_ORDER_DT_1YR,
                    ROUND(LTV_1YR.EXP_TRANS_1YR_LTV,2) as EXP_TRANS_1YR_LTV_1YR,
                    ROUND(LTV_1YR.EXP_TRANS_3YR_LTV,2) as EXP_TRANS_3YR_LTV_1YR,
                    ROUND(LTV_1YR.EXP_TRANS_5YR_LTV,2) as EXP_TRANS_5YR_LTV_1YR
                    FROM (
                    SELECT /*+ PARALLEL(8) */
                    s.customer_id,
                    /******** VARIABLE SFOR CLUSTERING *********/
                    --RAW FINANCIALS
                    --ROUND(sum(s.net_revenue),4) as net_revenue, --RETURNS BUILT IN, WANT PLACED ORDER METRICS
                    --ROUND(sum(s.gross_margin),4) as gross_margin, --RETURNS BUILT IN, WANT PLACED ORDER METRICS
                    --ROUND(sum(s.GROSS_MARGIN_PRODUCT),4) as gross_margin_product, --RETURNS BUILT IN, WANT PLACED ORDER METRICS
                    trunc(min(s.order_date_time),'IW')+7 as week_ending,
                    --CUSTOMER CLASSIFICATION METRICS
                    max(s.RETURN_ORDER_FLAG) as RETURN_ORDER_FLAG,
                    max(t.cust_class_chnnl_phone_flag) as cust_class_chnnl_phone_flag,
                    max(t.cust_class_chnnl_website_flag) as cust_class_chnnl_website_flag,
                    --max(t.cust_class_chnnl_omni_flag) as cust_class_chnnl_omni_flag, --NOT NEEDED FOR NC BECAUSE THEY CAN ONLY ORDER IN ONE CHANNEL
                    --max(case when upper(s.sales_SUBCHANNEL_NAME) = 'CALL CENTER' then 1 else 0 end) as phone_cust, --SHOULD BE REDUNDANT WITH cust_class_chnnl_phone_flag
                    --max(case when upper(s.sales_SUBCHANNEL_NAME) = 'JTV.COM' then 1 else 0 end) as website_cust, --SHOULD BE REDUNDANT WITH cust_class_chnnl_website_flag
                    --max(case when upper(s.sales_SUBCHANNEL_NAME) = 'AOS' then 1 else 0 end) as AOS_cust, --NOT NEEDED FOR NC BECAUSE THEY CANT PLACE FIRST ORDER ON AOS
                    max(case when upper(s.BROADCAST_INFLUENCED) = 'INFLUENCED' then 1 else 0 end) as BCAST_INFLUENCED_CUST,
                    max(case when upper(s.BROADCAST_INFLUENCED) = 'ANYTIME' then 1 else 0 end) as ANYTIME_CUST,
                    --RAW FINANCIALS - CAN BE USED FOR SUMMARY DATA AS WELL
                    ROUND(sum(s.CANCEL_RATE),4) as NUM_ORDERS,
                    ROUND(sum(s.GROSS_SALES),4) as GROSS_PRODUCT_SALES,
                    ROUND(SUM(s.PRODUCT_QUANTITY*s.cancel_rate),4) as PRODUCT_QTY,
                    ROUND(SUM(S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate),2) AS EXTENDED_PRODUCT_RETAIL_PRICE,
                    ROUND(SUM(S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate),2) as EXTENDED_PRODUCT_DISCOUNT, --DISCOUNT RATE => EXTENDED_PRODUCT_DISCOUNT / EXTENDED_PRODUCT_RETAIL_PRICE       SOLD UNIT PP => (EXTENDED_PRODUCT_RETAIL_PRICE + EXTENDED_PRODUCT_DISCOUNT) / PRODUCT_QTY
                    ROUND(SUM(S.PRODUCT_UNIT_COST*S.PRODUCT_QUANTITY*s.cancel_rate),2) as EXTENDED_PRODUCT_COST,--IMU => 1-(EXTENDED_PRODUCT_COST / EXTENDED_PRODUCT_RETAIL_PRICE)
                    ROUND(SUM(s.EXTENDED_SHIPPING_PRICE),4) as EXTENDED_SHIPPING_PRICE,
                    ROUND(SUM(s.EXTENDED_SHIPPING_DISCOUNT),4) AS EXTENDED_SHIPPING_DISCOUNT, --SHIPPING DISCOUNT RATE => EXTENDED_SHIPPING_DISCOUNT / EXTENDED_SHIPPING_PRICE
                    --CALCULATED FINANCIAL METRICS PER CUST
                    ROUND(SUM(S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate)+SUM(S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate)-SUM(S.PRODUCT_UNIT_COST*S.PRODUCT_QUANTITY*s.cancel_rate),2) AS PRODUCT_GROSS_MARGIN,
                    case when SUM(S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate)> 0 then ROUND(1-(SUM(S.PRODUCT_UNIT_COST*S.PRODUCT_QUANTITY*s.cancel_rate)/SUM(S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate)),4) else null end as IMU,
                    --RATE METRICS PER CUST
                    CASE WHEN SUM(S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) > 0 then ABS(ROUND(SUM(S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate)/SUM(S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate),4)) else null end as PRODUCT_DISCOUNT_RATE,
                    case when SUM(S.PRODUCT_QUANTITY*s.cancel_rate)>0 then ROUND((SUM(S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate)+SUM(S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate))/SUM(S.PRODUCT_QUANTITY*s.cancel_rate),2) else null end as PRODUCT_UNIT_PP,
                    CASE WHEN SUM(s.EXTENDED_SHIPPING_PRICE) > 0 then ABS(ROUND(SUM(s.EXTENDED_SHIPPING_DISCOUNT)/SUM(s.EXTENDED_SHIPPING_PRICE),4)) else null end as SHIPPING_DISCOUNT_RATE,
                    --PAYMENT TYPE
                    max(case when upper(s.PAYMENT_METHOD_SUBTYPE_CODE) = 'PA' then 1 else 0 end) as PA_CUST,
                    --DEPARTMENT METRICS
                    count(distinct s.DEPARTMENT_CURRENT) as num_depts,
                    max(CASE WHEN upper(s.department_current) = 'BELLA LUCE' then 1 else 0 end) as BELLA_CUST,
                    max(CASE WHEN upper(s.department_current) = 'BELLA LUCE DESIGNERS' then 1 else 0 end) as BELLA_DESIGNER_CUST,
                    max(CASE WHEN upper(s.department_current) = 'COSTUME JEWELRY' then 1 else 0 end) as COSTUME_CUST,
                    max(CASE WHEN upper(s.department_current) = 'COLOR GOLD' then 1 else 0 end) as COLOR_GOLD_CUST,
                    max(CASE WHEN upper(s.department_current) = 'GOLD' then 1 else 0 end) as GOLD_CUST,
                    max(CASE WHEN upper(s.department_current) = 'PEARLS' then 1 else 0 end) as PEARLS_CUST,
                    max(CASE WHEN upper(s.department_current) = 'SILVER' then 1 else 0 end) as SILVER_CUST,
                    max(CASE WHEN upper(s.department_current) = 'COLOR SILVER' then 1 else 0 end) as COLOR_SILVER_CUST,
                    max(CASE WHEN upper(s.department_current) = 'COLOR SILVER BRANDS' then 1 else 0 end) as COLOR_SILVER_BRANDS_CUST,
                    max(CASE WHEN upper(s.department_current) = 'DIAMOND GOLD' then 1 else 0 end) as DIAMOND_GOLD_CUST,
                    max(CASE WHEN upper(s.department_current) = 'DIAMOND SILVER' then 1 else 0 end) as DIAMOND_SILVER_CUST,
                    max(CASE WHEN upper(s.department_current) = 'DIAMOND SYNTHETIC' then 1 else 0 end) as DIAMOND_SYNTH_CUST,
                    max(CASE WHEN upper(s.department_current) NOT IN ('BELLA LUCE','BELLA LUCE DESIGNERS','COSTUME JEWELRY','COLOR GOLD','GOLD','PEARLS','SILVER','COLOR SILVER','COLOR SILVER BRANDS','DIAMOND GOLD','DIAMOND SILVER','DIAMOND SYNTHETIC') then 1 else 0 end) as OTHER_DEPT_CUST,

                    --FOR CLUSTER SUMMARY STATS
                    --PRODUCT SALES
                    sum(CASE WHEN upper(s.department_current) = 'BELLA LUCE' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as BELLA_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'BELLA LUCE DESIGNERS' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as BELLA_DESIGNER_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'COSTUME JEWELRY' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COSTUME_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR GOLD' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COLOR_GOLD_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'GOLD' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as GOLD_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'PEARLS' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as PEARLS_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'SILVER' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as SILVER_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR SILVER' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COLOR_SILVER_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR SILVER BRANDS' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COLOR_SILVER_BRANDS_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND GOLD' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as DIAMOND_GOLD_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND SILVER' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as DIAMOND_SILVER_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND SYNTHETIC' then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as DIAMOND_SYNTH_PROD_SALES,
                    sum(CASE WHEN upper(s.department_current) NOT IN ('BELLA LUCE','BELLA LUCE DESIGNERS','COSTUME JEWELRY','COLOR GOLD','GOLD','PEARLS','SILVER','COLOR SILVER','COLOR SILVER BRANDS','DIAMOND GOLD','DIAMOND SILVER','DIAMOND SYNTHETIC') then (S.PRODUCT_RETAIL_PRICE*S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as OTHER_DEPT_PROD_SALES,
                    --PRODUCT DISCOUNTS => CALCULATE DISC RATE => ABS(PRODUCT DISCOUNTS / PRODUCT SALES)
                    sum(CASE WHEN upper(s.department_current) = 'BELLA LUCE' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as BELLA_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'BELLA LUCE DESIGNERS' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as BELLA_DESIGNER_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'COSTUME JEWELRY' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as COSTUME_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR GOLD' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as COLOR_GOLD_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'GOLD' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as GOLD_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'PEARLS' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as PEARLS_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'SILVER' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as SILVER_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR SILVER' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as COLOR_SILVER_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR SILVER BRANDS' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as COLOR_SILVER_BRANDS_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND GOLD' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as DIAMOND_GOLD_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND SILVER' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as DIAMOND_SILVER_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND SYNTHETIC' then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as DIAMOND_SYNTH_PROD_DISC,
                    sum(CASE WHEN upper(s.department_current) NOT IN ('BELLA LUCE','BELLA LUCE DESIGNERS','COSTUME JEWELRY','COLOR GOLD','GOLD','PEARLS','SILVER','COLOR SILVER','COLOR SILVER BRANDS','DIAMOND GOLD','DIAMOND SILVER','DIAMOND SYNTHETIC') then (S.EXTENDED_PRODUCT_DISCOUNT*s.cancel_rate) else 0 end) as OTHER_DEPT_PROD_DISC,
                    --PRODUCT QTY => CALCULATE SOLD UNIT PP => (PRODUCT SALES + PRODUCT DISCOUNTS) / PRODUCT QTY
                    sum(CASE WHEN upper(s.department_current) = 'BELLA LUCE' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as BELLA_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'BELLA LUCE DESIGNERS' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as BELLA_DESIGNER_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'COSTUME JEWELRY' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COSTUME_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR GOLD' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COLOR_GOLD_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'GOLD' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as GOLD_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'PEARLS' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as PEARLS_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'SILVER' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as SILVER_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR SILVER' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COLOR_SILVER_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'COLOR SILVER BRANDS' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as COLOR_SILVER_BRANDS_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND GOLD' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as DIAMOND_GOLD_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND SILVER' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as DIAMOND_SILVER_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) = 'DIAMOND SYNTHETIC' then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as DIAMOND_SYNTH_PROD_QTY,
                    sum(CASE WHEN upper(s.department_current) NOT IN ('BELLA LUCE','BELLA LUCE DESIGNERS','COSTUME JEWELRY','COLOR GOLD','GOLD','PEARLS','SILVER','COLOR SILVER','COLOR SILVER BRANDS','DIAMOND GOLD','DIAMOND SILVER','DIAMOND SYNTHETIC') then (S.PRODUCT_QUANTITY*s.cancel_rate) else 0 end) as OTHER_DEPT_PROD_QTY

                    from BA_SCHEMA.SALES s
                    left outer join BA_SCHEMA.CUSTOMER_CLASSIFICATION_STATUS t
                    on nvl(s.order_reference_number,s.ORDER_NUMBER) = t.order_reference_number
                    where s.order_date_time >= to_date('01-jan-", year_i, "')-364
                    AND s.order_date_time < to_date('01-jan-", year_i, "')
                    AND upper(s.sales_SUBCHANNEL_NAME) NOT IN ('AUCTION', 'EMPLOYEE','CUSTOMER SERVICE','UNKNOWN')
                    AND t.ORD_CUST_CLASS_STATUS_NEW_FLAG = 1
                    AND S.PRODUCT_QUANTITY*s.cancel_rate > 0
                    group by s.customer_id
                    ) SALES
                    left outer join BA_SCHEMA.CUSTOMER_LIFETIME_VALUE LTV
                    on SALES.customer_id = LTV.customer_id
                    and SALES.week_ending = LTV.week_of
                    left outer join BA_SCHEMA.CUSTOMER_LIFETIME_VALUE LTV_1YR
                    on SALES.customer_id = LTV_1YR.customer_id
                    and SALES.week_ending+(7*52) = LTV_1YR.week_of
                    ;
                    ", sep = "")

      data = dbGetQuery(conn, dat_q)
      return(data)
    }


  }else{
    message("Probably a smart decision.")
  }


}
