#' Quickly perform clustering and modeling with a command-line interface
#'
#' \code{perform_clustering} performs Hierarchical DBSCAN clustering on numeric
#' data. Cluster assignments are then used as the dependent classifier for a
#' random forest.
#'
#' This function wraps clustering and modeling into a single procedure. It
#' performs clustering on numeric data using HDBSCAN. The cluster assignments
#' are then utilized as classifiers to train a random forest model. The random
#' forest object is then capable of classifying novel observations.
#'
#' The order of operations for this function is: \enumerate{ \item Stratified
#' sampling of input data by \emph{stratiferColumn} and \emph{train_sample_size}
#' \item H-DBSCAN clustering using \emph{min_cluster_size} \item Random forest
#' modeling using \emph{num_trees} as number of trees in the forest }
#'
#' @param df The input data frame of observations to be clustered and modeled.
#'   Data frame should consist of numeric columns only and can be output of
#'   \code{\link{process_data}}.
#' @param stratifierColumn The column within the data frame that controls
#'   stratified random sampling. This column will be excluded from clustering
#'   and modeling. Must be a factor variable.
#' @param train_sample_size The decimal percent of total observations to be
#'   sampled for clustering. The clustering algorithm is RAM-intensive, so this
#'   parameter may need tuning to ensure the process does not fail due to memory
#'   restrictions.
#' @param min_cluster_size The minimum number of observations that constitute a
#'   valid cluster. This is the only required input for the H-DBSCAN clustering
#'   algorithm.
#' @param num_trees The number of constituent trees used to build the random
#'   forest model. Increasing the number of trees may increase the stability of
#'   the model solution.
#'
#' @return The output will be a list containing the original data frame
#'   \emph{orig_df}, the randomly selected observations used for clustering and
#'   to train the random forest model \emph{df_sample}, the cluster object
#'   \emph{cluster_obj}, and the random forest model object
#'   \emph{randomForest_model}.
#'
#' @examples
#'  stratifier = df[, 1] #stratify by values in first column
#'  train = 0.1 #train on 10% of total observations
#'  min_cluster_size = 1000 #minimum size of valid clusters is 1000 points
#'  num_trees = 2000 #create 2000 bootstrapped and boosted trees in the random forest model
#'  out = perform_clustering(df, stratifierColumn = stratifier, train_sample_size = train, min_cluster_size = min_cluster_size, num_trees = num_trees)
#'
#'
#'  \dontrun{
#'    randomF_model = out$randomForest_model
#'    traim_samples = out$df_split
#'    test_samples = df[-train_samples, ]
#'    cluster_solution = out$cluster_obj
#'  }
#'
#'

perform_clustering = function(df, stratifierColumn, train_sample_size, min_cluster_size, num_trees){

  message("Testing to ensure input dataframe is numeric...")
  if(all(sapply(df,is.numeric))){

    message("All columns are numeric. Proceeding with clustering.")
    message("Creating stratified sample of input df for clustering.")
    df$stratifierColumn = as.factor(df$stratifierColumn)

    # TAKE 10% STRATIFIED SAMPLE OF INPUT DF FOR CLUSTERING
    splitIndex = createDataPartition(df$stratifierColumn,
                                     p= (1-train_sample_size))
    # EXTRACT SAMPLE OF
    df_split = df[-splitIndex, ]

    # PERFORM CLUSTERING WITH HDBSCAN
    min_N = min_cluster_size
    clust1 = hdbscan(df_split, min_N)
    df_split$cluster = clust1$cluster

    # USE RANDOM FOREST TO EXTRACT VARIABLES
    mtry = round(sqrt(ncol(dat_split)))
    rf1 = randomForest(cluster ~ ., data = df_split, ntree = num_trees)


  }else{

    message("Error MSG: Please ensure that all dataframe columns are numeric before proceeding.")

  }

  return(list(orig_df = df, df_sample = df_split, randomForest_model = rf1, cluster_obj = clust1))

}
