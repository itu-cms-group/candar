# PARALLEL VERSION, CPU time is reduced by ~30% (on 4 threaded computer) compared to serial version of this same function
# input is zip code. easier search of coordinates, this function returns just lat and lon
par.zip.coordinates <- function(x,nthreads = NULL){
  require(parallel)
  if(is.null(nthreads)) nthreads <- detectCores() # if not specified, use all available cores
  if(!exists("Zips")) Zips <- GetZips() # if zip database does not exists, import it.
  cl <-  makeCluster(nthreads) # initiate a cluster
  lapply(c("Zips","x"), function(n) clusterExport(cl,n)) # export variables
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
  lapply(c("Zips","x"), function(n) clusterExport(cl,n)) # export variables
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
  Result[,c(1,2)] <- par.zip.coordinates(data[,col.sen],nthreads)#[,c(1,2)] # find coordinates of sender zips
  Result[,c(3,4)] <- par.zip.coordinates(data[,col.rec],nthreads)#[,c(1,2)] # find coordinates of receipent zips
  
  # get location names from zips
  Result[,c(5,6)] <- par.zip.location(data[,col.sen],nthreads)#[,c(1,2)] # find city/state of sender zips
  Result[,c(7,8)] <- par.zip.location(data[,col.rec],nthreads)#[,c(1,2)] # find city/state of receipent zips
  
  # calculate the distance between supplier and customer
  Dist <- distHaversine(Result[,2:1],Result[,4:3]) # in meters
  Result[,9] <- round(Dist/1000,3) # as kilometers
  return(Result)
}

# PARALLEL VERSION
# this function is for filtering and gathering location information about shipping data
par.Filter.ShippingData <- function(Ship, # input raw shipping data after it is formatted with Format.ShipData()
                                    location.info=TRUE, # in order to collect location lat/lon info about a row
                                    file.write=TRUE, # true if you want to save to csv file
                                    file.name="Shipping_Filtered.csv", # name of the file to be written
                                    nthreads = NULL){
  
  # Filter
  Result <- Ship[-which(Ship$ShippingCost==0),] # filter by cost, exclude transactions with no cost
  Result <- Result[-which(Result$Type == "UPS Ground"),] # filter by type, exclude UPS Ground shipping
  
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
