### --------------------------------------------------------------------------------- ###
### - Contains function recipes for analyzing second half data of 2012 of Company B - ###
### - Author     : Mert Candar ------------------------------------------------------ ###
### - Study      : Predictive Modelling with Machine Learning ----------------------- ###
### - Class      : Graduation Project ----------------------------------------------- ###
### - Supervisor : Dr.Altan Cakir --------------------------------------------------- ###
### - Department of Physics Engineering, Istanbul Technical University -------------- ###
### - Istanbul, Turkey -------------------------------------------------------------- ###
### --------------------------------------------------------------------------------- ###

# PARALLEL COMPUTATION LIBRARY FOR GRADUATION PROJECT

# source("https://raw.githubusercontent.com/itu-cms-group/candar/master/parlib.R")

# PARALLEL VERSION, CPU time is reduced by ~30% (on 4 threaded computer) compared to serial version of this same function
# input is zip code. easier search of coordinates, this function returns just lat and lon
par.zip.coordinates <- function(x,nthreads = NULL){
  require(parallel)
  if(is.null(nthreads)) nthreads <- detectCores() # if not specified, use all available cores
  if(!exists("Zips")) Zips <- GetZips() # if zip database does not exists, import it.
  cl <-  makeCluster(nthreads) # initiate a cluster
  clusterExport(cl,c("Zips","x")) # export variables
  Result <- as.data.frame(matrix(I(unlist(
    parLapply(cl,x,function(n)Zips[match(n,Zips$Postal.Code),c(6,7)]) # parallel computation for lapply()
  )),length(x),2,byrow = TRUE)) # find and convert result to data frame
  stopCluster(cl)
  colnames(Result) <- c("Lattitude","Longitude")
  return(cbind(as.numeric(as.character(Result[,1])),as.numeric(as.character(Result[,2]))))
}

# PARALLEL VERSION, CPU time is reduced by ~30% and up to ~80% (on 4 threaded computer) 
# compared to serial version of this same function
# input is a zip code. prepare a function for easier city and state matching
par.zip.location <- function(x,nthreads = NULL){
  require(parallel)
  if(is.null(nthreads)) nthreads <- detectCores() # if not specified, use all available cores
  if(!exists("Zips")) Zips <- GetZips() # if zip database does not exists, import it.
  cl <-  makeCluster(nthreads) # initiate a cluster
  clusterExport(cl,c("Zips","x")) # export variables
  Result <- as.data.frame(matrix(unlist(
    parLapply(cl,x,function(n)Zips[match(n,Zips$Postal.Code),c(2,4)]) # parallel computation for lapply()
  ),length(x),2,byrow = TRUE)) # find and convert result to data frame
  stopCluster(cl)
  colnames(Result) <- c("Lattitude","Longitude")
  return(cbind(as.character(Result[,1]),as.character(Result[,2])))
}

# PARALLEL VERSION
# find lat,lon,city,state and distance values for a given set of zip codes
# initially, the content of the function was a part of partial.shipdata() function but later
# it was extracted to make available for external uses.
par.LocationData <- function(data,       # can be raw shipdata or saledata, or any type of data containing zip codes
                             col.sen=4,  # column number of sender zips
                             col.rec=5,  # column number of receipent zips
                             nthreads = NULL){ # number of cores to use
  require(parallel)
  require(geosphere)
  if(!exists("Zips")) Zips <- GetZips() # if zip database does not exists, import it.
  Result <- as.data.frame(matrix(NA,nrow(data),9)) # preallocate output matrix
  colnames(Result) <- c("S.Lat","S.Lon","R.Lat","R.Lon","S.City","S.StateCode","R.City","R.StateCode","Distance")
  
  # get coordinates as lat and lon from zips
  Result[,c(1,2)] <- par.zip.coordinates(data[,col.sen],nthreads) # find coordinates of sender zips
  Result[,c(3,4)] <- par.zip.coordinates(data[,col.rec],nthreads) # find coordinates of receipent zips
  
  # get location names from zips
  Result[,c(5,6)] <- par.zip.location(data[,col.sen],nthreads) # find city/state of sender zips
  Result[,c(7,8)] <- par.zip.location(data[,col.rec],nthreads) # find city/state of receipent zips
  
  # calculate the distance between supplier and customer
  Dist <- distHaversine(Result[,2:1],Result[,4:3]) # in meters
  Result[,9] <- round(Dist/1000,3) # as kilometers
  return(Result)
}

# PARALLEL VERSION
# this function is for filtering and gathering location information about shipping data
par.Filter.ShippingData <- function(Ship, # input raw shipping data after it is formatted with Format.ShipData()
                                    filter = FALSE,
                                    location.info=TRUE, # in order to collect location lat/lon info about a row
                                    file.write=TRUE, # true if you want to save to csv file
                                    file.name="Shipping_Filtered.csv", # name of the file to be written
                                    nthreads = NULL){
  
  Result <- Ship
  # Filter
  if(filter){
    Result <- Ship[-which(Ship$ShippingCost==0),] # filter by cost, exclude transactions with no cost
    Result <- Result[-which(Result$Type == "UPS Ground"),] # filter by type, exclude UPS Ground shipping
  }
  
  # Collect data for locations
  if(location.info) Result <- cbind(Result,par.LocationData(Result,nthreads=nthreads)) # column-bind them together
  
  # Export to csv file
  if(file.write){
    write.csv(Result,file.name,row.names = FALSE)
    cat("File",file.name,"is saved to",getwd(),"\n")
  }
  return(Result)
}
# PARALLEL VERSION, need improvements at do.call section
# input "source" as a vector or a list
#############
par.Search.List <- function(source, # object to be searched
                            target, # object expected to include source at least once
                            col,    # target column
                            nthreads = NULL){ # number of cores to use
  
  if(is.null(nthreads)) nthreads = detectCores()
  require(parallel)
  cl <- makeCluster(nthreads) # initiate cluster
  clusterExport(cl,c("source","target","col")) # export variables to the cluster
  
  Result <- parSapply(cl,source,function(x){
    index <- which(target[,col]==x) # search the source in target, get indexes
    if(length(index)!=0)
      sapply(index, function(i) c(x,target[i,])) # THIS IS OKAY JUST NEEDS A CONVERTION TO DATA FRAME
  })
  
  Result <- parLapply(cl, parLapply(cl,Result, function(y)
    as.data.frame(matrix(y,length(y)/(ncol(target)+1),ncol(target)+1,byrow = T)))
    ,data.frame, stringsAsFactors=FALSE)
  stopCluster(cl) # stop cluster because the rest is serial
  
  # convert to data frame from list, following is time consuming and is NOT parallel
  Result <- do.call(rbind, Result) ## THIS WORKS
  colnames(Result) <- c("Source",colnames(target))
  
  return(Result)
}

# PARALLEL VERSION, need improvements at do.call
# input "source" as a data frame
par.Match.rows <- function(source,  # source data frame, this will be searched inside target
                           col.sou, # column number of source data to be searched
                           target,  # target data frame, expected to have source data frame's values
                           col.tar, # column number of target data frame to be inspected
                           nthreads = NULL # number of cores
){
  if(is.null(nthreads)) nthreads = detectCores()
  require(parallel)
  cl <- makeCluster(nthreads) # initiate cluster
  clusterExport(cl,c("source","target","col.tar","col.sou")) # export variables to the cluster
  
  Result <- parLapply(cl,seq(nrow(source)),function(j){
    index <- which(source[j,col.sou]==target[,col.tar]) # search the source in target, get indexes
    if(length(index)!=0)
      sapply(index, function(i) c(source[j,],target[i,])) # THIS IS OKAY JUST NEEDS A CONVERTION TO DATA FRAME
    else
      c(source[j,],rep(NA,ncol(target))) # try with ifelse of base or if_else of dplyr
  })
  
  Result <- parLapply(cl, parLapply(cl,Result, function(y) # need arrangement
    as.data.frame(matrix(y,length(y)/(ncol(target)+ncol(source)),ncol(target)+ncol(source),byrow = T)))
    ,data.frame, stringsAsFactors=FALSE)
  stopCluster(cl)
  
  Result <- do.call(rbind, Result) ## THIS WORKS
  colnames(Result) <- c(colnames(source),colnames(target))
  return(Result)
}

# # PARALLEL VERSION
# # performs matching of filtered (rearranged) ship data and raw sale data
# par.Match.ShipData <- function(shipfiltered, # ultimate filtered and location data included shipping data 20 columns
#                                month, # input a month number as integer
#                                saledata, # raw sale data e.g. Month10.txt, Month11.txt etc.
#                                iterations = 10, # number of part to divide into when computing
#                                filename = "Most_Expensive_Matched_M_", # file name to be saved
#                                begin = 1, # beginning of the for loop, could be continued from other steps
#                                ship.sonumber = 8, # sonumber column in filtered shipping data
#                                sale.sonumber = 1, # sonumber column in filtered sale data
#                                nthreads = NULL # number of cores to use
# ){
#   require("lubridate")
#   # oninit: create a directory for tidier work
#   main_path <- getwd() # "C:/Users/USER/Desktop/R"
#   current_path <- paste(getwd(),paste("DataMatch",sample(100,1),sep="_"),sep = "/") # folder names
#   dir.create(current_path) # create a new directory to store files
#   setwd(current_path) # set new working directory
#   cat("Working directory is set to",current_path,"\n")
#   
#   # pull out just one month, and sort according to shipping cost
#   temp_ship <- Sort(shipfiltered[which(month(shipfiltered$DateShipped)==as.integer(month)),],7,decreasing = TRUE)
#   cat("Total",nrow(temp_ship),"rows temp_ship (filtered ship data) has.\n")
#   cat("Filtered shipping data is fractionated according to month.\n")
#   
#   # at following line temp_ship[,ship.sonumber] is changed to as.integer(levels(factor(temp_ship[,ship.sonumber])))
#   temp_raw <- par.Search.List(as.integer(levels(factor(temp_ship[,ship.sonumber]))),
#                               saledata,
#                               sale.sonumber,
#                               nthreads = nthreads)[,-1] # get corresponding rows of saledata, by sonumber
#   print(temp_raw)
#   cat("Temporary fractional sale data is formed with corresponding SONumbers.\n")
#   
#   rows <- nrow(temp_ship) # get number of rows to use in following for loop
#   cat("Levels of temp_ship SONumber :",length(levels(factor(temp_ship[,ship.sonumber]))),
#       "total number of rows in temp_ship :",nrow(temp_ship),"\n")
#   
#   ## divide into fractions and then unify into one big file
#   for(i in begin:iterations){
#     pb <- txtProgressBar(style = 3)
#     cat("Step",i,"\n")
#     index <- seq((i-1)*(rows/iterations)+1,i*(rows/iterations)) # determine interval of indexes
#     test_match <- par.Match.rows(temp_ship[index,],
#                                  ship.sonumber,
#                                  temp_raw,
#                                  sale.sonumber,
#                                  nthreads = nthreads) # match data and bind together as a data frame
#     check_sen <- test_match[,4]==test_match[,23] & 
#       test_match[,5]==test_match[,24] # check receipent and sender zips whether they match
#     test_match <- test_match[check_sen,] # take only who match by zips
#     write.csv(test_match,paste(filename,i,".csv",sep = ""),row.names = FALSE) # write to file with order
#     cat("File",paste(filename,i,".csv",sep = ""),"is saved to",getwd(),"\n") # inform user
#     setTxtProgressBar(pb, i/iterations)
#   }
#   
#   Result <- data.frame() # combine results into one data frame
#   for(i in 1:iterations){
#     Result <- rbind(Result,read.csv(paste(filename,i,".csv",sep = ""),row.names = NULL))
#     cat(i,"th file is imported to finally merge together.\n")
#   }
#   
#   setwd(main_path) # reset working directory
#   write.csv(Result,paste(filename,".csv",sep = ""),row.names = FALSE)
#   cat("File",paste(filename,".csv",sep = ""),"is saved to",getwd(),"\n") # inform user
#   
#   # ondeinit: remove the directory used for temporary storage
#   unlink(current_path,recursive = TRUE,force = TRUE)
#   
#   return(Result)
# }

# PARALLEL VERSION
# performs matching of filtered (rearranged) ship data and raw sale data
par.Match.ShipData <- function(shipfiltered, # ultimate filtered and location data included shipping data 20 columns
                               month, # input a month number as integer
                               saledata, # raw sale data e.g. Month10.txt, Month11.txt etc.
                               iterations = 10, # number of part to divide into when computing
                               filename = "Most_Expensive_Matched_M_", # file name to be saved
                               begin = 1, # beginning of the for loop, could be continued from other steps
                               ship.sonumber = 8, # sonumber column in filtered shipping data
                               sale.sonumber = 1, # sonumber column in filtered sale data
                               nthreads = NULL # number of cores to use
){
  # force(saledata)
  require("lubridate")
  require("parallel")

  # pull out just one month, and sort according to shipping cost
  temp_ship <- Sort(shipfiltered[which(month(shipfiltered$DateShipped)==as.integer(month)),],7,decreasing = TRUE)
  cat("Total",nrow(temp_ship),"rows temp_ship (filtered ship data) has.\n")
  cat("Filtered shipping data is fractionated according to month.\n")
  print(temp_ship[1:3,])

  # at following line temp_ship[,ship.sonumber] is changed to as.integer(levels(factor(temp_ship[,ship.sonumber])))
  temp_raw <- par.Search.List(as.integer(levels(factor(temp_ship[,ship.sonumber]))), # source
                              saledata, # target
                              sale.sonumber, # col
                              nthreads = nthreads)[,-1] # get corresponding rows of saledata, by sonumber
  print(temp_raw[1:3,])
  cat("Temporary fractional sale data is formed with corresponding SONumbers.\n")
  
  rows <- nrow(temp_ship) # get number of rows to use in following for loop
  cat("Levels of temp_ship SONumber :",length(levels(factor(temp_ship[,ship.sonumber]))),
      "total number of rows in temp_ship :",nrow(temp_ship),"\n")
  Result <- data.frame() # combine results into one data frame
  ## divide into fractions and then unify into one big file
  pb <- txtProgressBar(style = 3)
  for(i in begin:iterations){
    index <- seq((i-1)*(rows/iterations)+1,i*(rows/iterations)) # determine interval of indexes
    test_match <- par.Match.rows(temp_ship[index,],
                                 ship.sonumber,
                                 temp_raw,
                                 sale.sonumber,
                                 nthreads = nthreads) # match data and bind together as a data frame
    check_sen <- as.integer(test_match[,4])==as.integer(test_match[,23]) & 
      as.integer(test_match[,5])==as.integer(test_match[,24]) # check receipent and sender zips whether they match

    test_match <- test_match[check_sen,] # take only who match by zips
    Result <- rbind(Result,test_match)
    setTxtProgressBar(pb, i/iterations)
  }
  return(Result)
}
