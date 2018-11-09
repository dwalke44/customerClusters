#' Quickly perform data pre-processing with a command-line interface
#'
#' \code{process_data} returns several data frames with various levels of
#'   pre-processing.
#'
#' This function wraps several individual pre-processing steps into a single
#' function and is driven by user input at the command line. The purpose of this
#' function is to quickly ensure that data is in the proper format for
#' clustering + random forest modeling. This function returns 3 data frames with
#' varying degrees of pre-processing.
#'
#' @section Testing for numeric data:
#'   Most clustering algorithms, such as the included H-DBSCAN, require input data
#'   to be numeric-type only. This function will return an error if input
#'   dataframe contains columns of types other than numeric.
#'
#' @section Handling for NAs:
#'   After the input dataframe has passed the numeric test, this function tests
#'   for the presence of NAs. If NAs are detected, the function assumes they
#'   represent zeroes and performs the appropriate replacement.
#'
#' @section Handling correlated predictors:
#'   Once NAs have been removed from the data frame, the function test for
#'   correlated predictors. The function will identify highly correlated
#'   predictors (default setting = >95% correlation) and prompt the user to
#'   identify which predictors to remove. If no predictors are considered
#'   candidates for removal, users should enter 0.
#'
#' @section Centering and scaling predictors:
#'   The final processing step in this function is to center and scale the
#'   predictors using the base-R scale function. Centered and scaled predictors
#'   are usually required for clustering, and often required for ML algorithms.
#'
#' @param df The input data frame for processing. Data frame should consist of
#'   numeric columns only.
#'
#' @return  The returned data frames are 1) \emph{drop_corr_var}, the input data frame
#'   with correlated variables removed; 2) \emph{corr_removed_cs}, the centered and
#'   scaled data frame without correlated variables; and 3) \emph{corr_present_cs}, the
#'   centered and scaled data frame including any correlated variables. The
#'   content of the output is dependent on selections made by the user.
#'
#' @examples
#'   out = process_data(df)
#'
#'   \dontrun{
#'   #example selection from output
#'   df1 = out$drop_corr_var
#'   df2 = out$corr_removed_cs
#'   df3 = out$corr_present_cs
#'   }
#' @docType function






process_data = function(df){

  output = list() #RETURN SEVERAL DATAFRAMES

  message("Testing if df has all numeric dataframes.")
  if(all(sapply(df,is.numeric))){
    #TEST FOR NAs, REPLACE ANY WITH ZEROES
    message("All columns numeric. Testing for NAs.")
    na_present = sum(colSums(is.na(df)))
    if(na_present > 0){
      message("NAs detected in columns. Automatically replacing with zeroes.")
      df[is.na(df)]<- 0
    }else{
      message("No NAs detected.")
    }

    #TEST FOR CORRELATED PREDICTORS
    message("Testing for correlated predictors.")
    datCor = cor(df)
    highCorr = sum(abs(datCor[upper.tri(datCor)])>0.95) #
    vars_for_removal = findCorrelation(datCor, cutoff = 0.95)
    print(vars_for_removal) #4,EXTENDDED PRODUCT RETAIL PRICE
    print(colnames(df))
    drop_corrs = readline(prompt = "These variables are highly (>95%) correlated. Ok to remove? Y/n...")
    if(drop_corrs == 'Y'){
      keep_going <- "Y"
      i = 1
      while(keep_going != "n"){
        vars_to_drop[i] = as.numeric(readline(prompt = "Drop which variables? Enter column index number for first variable. "))
        keep_going = readline(prompt = "Additional variables to drop? Y/n  ")
        i = i+1
      }
      df_drop_corr_var = df[ , -(vars_to_drop)]
      df_drop_corr_var = as.tibble(df_drop_corr_var)
      #CENTER AND SCALE PREDICTORS WITH CORRELATED PREDICTORS REMOVED
      message("Centering and scaling predictors without correlated variables.")
      df_corr_removed_cs = scale(df_drop_corr_var, center = TRUE, scale = TRUE)
      df_corr_removed_cs = as.tibble(df_corr_removed_cs)
    }else{
      message("Correlated variables not removed. Proceed with caution.")
      df_drop_corr_var = as.tibble(matrix(0))
      df_corr_removed_cs = as.tibble(matrix(0))
      c_and_s_all = readline(prompt = "Continue with centering and scaling? Y/n ...")
      if(c_and_s_all == "Y"){
        df_corr_present_cs = scale(df, center = TRUE, scale = TRUE)
        df_corr_present_cs = as.tibble(df_corr_present_cs)
        message("Centering and scaling completed on all predictors.")
      }else{
        message("No processing performed on data. End of processing procedure.")
        df_corr_present_cs = as_tibble(matrix(0))
      }
    }




  }else{
    message("Error MSG: Please ensure that all dataframe columns are numeric before proceeding.")
  }

  return(list(drop_corr_var = df_drop_corr_var, corr_removed_cs = df_corr_removed_cs, corr_present_cs = df_corr_present_cs))


}
