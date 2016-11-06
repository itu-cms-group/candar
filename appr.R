### --------------------------------------------------------------------------------- ###
### - Contains function recipes for analyzing second half data of 2012 of Company B - ###
### - Author     : Mert Candar ------------------------------------------------------ ###
### - Study      : Predictive Modelling with Machine Learning ----------------------- ###
### - Class      : Advanced Physics Project Lab ------------------------------------- ###
### - Supervisor : Altan Cakir ------------------------------------------------------ ###
### - Department of Physics Engineering, Istanbul Technical University -------------- ###
### - Istanbul, Turkey -------------------------------------------------------------- ###
### --------------------------------------------------------------------------------- ###

## Copy-paste following line to import the content directly from github ##
# source("https://raw.githubusercontent.com/mcandar/Agents/master/appr.R")

# Get zip information from github, for mapping purposes
GetZips <- function(){
  Zips <- read.table("https://raw.githubusercontent.com/mcandar/Agents/master/US_Postal_Codes_Merged.txt",
                     colClasses = "character")
  return(Zips)
}

# Row-wise sorting, ascending order. Just input data and column number you want to sort
Sort <- function(data,col,decreasing = FALSE){
  return(data[order(data[,col],decreasing=decreasing),])
}

# Easily set column names for both sale data (Month10.txt, etc.) and shipping data (Shipp..nths10to12.txt, etc)
SetColNames <- function(data,type = "sale.data"){
  switch (type,
    "sale.data" = {
      colnames(data) <- c("SONumber","ShippingDate","SenderZip","ReceipentZip","ItemDescription","ItemWeight",
                          "UnitsShipped","AverageUnitPrice")
    },
    "shipping.data" = {
      colnames(data) <- c("TrackingNumber","Company","ShippingCode","SenderZip","ReceipentZip","Type",
                          "ShippingCost","SONumber","DateShipped","DateDelivered","Duration")
    }
  )
  return(data)
}

# Import time series into R
GetTime <- function(data,col,format="%m/%d/%Y %H:%M:%S"){
  Result <- data # back up the data
  # Result[[col]] <- strptime(data[[col]],format=format) # convert date and time from characters
  for (i in col)
    Result[[i]] <- strptime(data[[i]],format=format) # convert date and time from characters
  return(Result)
}

# convert classes with minimal possible loss of information
Convert <- function(data,col,class="numeric",split=FALSE,na.rm=FALSE,limupper=0){ # unfactors and extracts the content out of it as numeric
  Result <- data
  for(i in col){ # split XXXXX-XXXX type of zip data, take the left side
    if(split){
      m <- strsplit(as.character(Result[,i]),"-") # separate them by "-"
      n <- as.data.frame(matrix(lapply(m, "[",1))) # take the left side
      Result[,i] <- as.data.frame(trimws(n[[1]])) # trim white spaces
    }
    else
      Result[,i] <- as.data.frame(trimws(Result[,i])) # trim white spaces without splitting
    
    switch (class,
            "numeric" = {
              Result[,i] <- as.numeric(as.character(Result[,i])) # originally denoted as Result$`Receipent Zip`
            },
            "character" = {
              Result[,i] <- as.character(Result[,i])
            }
    )
  }
  if(na.rm){ # remove NA rows and calculate the percentage
    oldr <- nrow(Result)
    Result <- na.omit(Result)
    if(class == "numeric"){
      for(i in col){ # remove the incorrect zips, zip codes cannot be equal or greater than 10^5
        if(limupper != 0 && any(Result[,i] > limupper))
          Result <- Result[-which(Result[,i] > limupper),]
      }
    }
    newr <- nrow(Result)
    cat(((oldr-newr)/oldr)*100,"% of the rows are removed.\n",sep = "")
  }
  return(Result)
}


# Draw quick graphs with base plotting function
MultiPlot <- function(x,                    # data of x axis
                      data,                 # data set of y axis
                      xlab = "Index",       # x label
                      main = "Plot",        # main title
                      fname = "Plot_",      # prefix for filename
                      method = "plot",      # two options, for ono-to-one: "plot", one-to-many: "matplot"                      
                      width = 1280,         # width as pixels
                      height = 720){        # height as pixels
  
  for(i in 1:ncol(data)){
    filename <- paste(fname,"_",i,"_",colnames(data[i]),"_vs_",xlab,".png",sep = "") #paste(name,i,".png",sep = "")
    png(filename,width = width,height = height)
    switch (method, # switch for different plot options
            "plot" = { # plot one column versus another one
              plotname <- paste(main," #",i,sep = "")
              plot(x,
                   data[,i],
                   xlab = xlab,
                   ylab = colnames(data[i]),
                   main = plotname,
                   cex.lab = 1.5)
            },
            "matplot" = { # plot one column versus rest of the columns
              plotname <- paste(main,"#",i,sep = "")
              matplot(data[,i],
                      data[,-c(i)], # exclude x axis object
                      xlab = colnames(data[i]),
                      ylab = colnames(data[-c(i)]), 
                      main = plotname,col = 1:(ncol(data)-1),
                      pch=1:(ncol(data)-1))
              legend("topleft",legend = colnames(data[-c(i)]), 
                     col=1:(ncol(data)-1),pch=1:(ncol(data)-1))
            }
    )
    grid()
    dev.off()
    cat("Image",filename,"saved to",getwd(),"\n")
  }
  return (TRUE)
}

# Draw multiple higher quality graphs with ggplot2, inputs can be multi-column data frames
# color input may not work properly, and it does not add color parameter name to legend
MultiGGPlot <- function(x,                    # data of x axis
                        data,                 # data set of y axis
                        main = "GGPlot",      # main title
                        fname = "GG_",        # prefix for filename
                        geom = "jitter",      # geometry of the plot
                        colour = "black",     # colour scale
                        colourname = "colour",# title of the legend
                        alpha = I(1/10),      # transparecy of the points
                        device = "png",       # choose the extension of the file
                        smooth = FALSE,       # de/activate smoothing
                        smethod = "lm",       # smoothing method
                        formula=y ~ poly(x, 3, raw=TRUE), # formula for smoothing, linear, polynomial etc.
                        width = 6,            # width as inches
                        height = 3.375){      # height as inches
  
  require(ggplot2)
  if(is.null(colnames(x))) 
    x <- as.data.frame(x) # convert to data frame if not already, (x maybe a multicolumn object too)
  
  for(j in 1:ncol(x)){ # 1 through total column number (originally 1:dim(x)[2])
    for(i in 1:ncol(data)){
      if (identical(x[,j],data[,i])) next()
      filename <- paste(fname,"_",i+((j-1)*ncol(data)),"_",colnames(data[i]),"_vs_",colnames(x[j]),".",device,sep = "") # form a file name with an index
      plotname <- paste(main," #",i,sep = "")
      ggplot(data, 
             aes(x = x[,j],
                 y = data[,i],
                 colour = colour)) +
        
        scale_colour_gradientn(colours=rainbow(4)) +
        
        switch (geom,
                "jitter" = {geom_jitter(alpha = alpha)},
                "point" = {geom_point(alpha = alpha)},
                "path" = {geom_path(alpha = alpha)}
        ) +
        
        labs(title = main,
             x = colnames(x[j]),
             y = colnames(data[i]),
             colour = colourname) +
        
        if(smooth)
          geom_smooth(method=smethod, se=TRUE, fill=NA,
                      formula=formula,colour="blue")
      
      ggsave(filename,device = device,width = width,height = height)
      cat("Image",filename,"saved to",getwd(),"\n")
    }
  }
  return (TRUE)
}

# for quick arranging of Month10.txt and similars
Format.SaleData <- function(data,                 # sale data e.g. Month10.txt, Month11.txt etc.
                            zipclass="character", # class of Sender and Receipent Zips
                            soclass="numeric",    # class of SONumbers
                            na.rm = FALSE,        # remove NA rows
                            ziplim = 10^5,        # upper limit of zips, greater is invalid
                            solim = 0){           # upper limit of SONumber, greater is invalid,zero = no limit
  Result <- GetTime(data,2) # get time series
  Result <- Convert(Result,c(3,4),class=zipclass,na.rm = na.rm,limupper = ziplim,split = TRUE) # filter zips
  Result <- Convert(Result,5,class="character",na.rm = na.rm,limupper = 0) # description column
  Result <- Convert(Result,c(6,7,8),na.rm = na.rm,limupper = ziplim) # filter weight,units,price
  Result <- Convert(Result,1,class=soclass,na.rm = na.rm,limupper = solim) # filter SOnumbers
  Result <- SetColNames(Result) # set column names
  return(Result)
}

# for quick arranging of ShippingData_Months_10to12.txt and similars
Format.ShippingData <- function(data,                 # shipping data e.g. ShippingData_Mont....txt etc.
                                zipclass="character", # class of Sender and Receipent Zips
                                soclass="numeric",    # class of SONumbers
                                na.rm = FALSE,        # remove NA rows
                                ziplim = 10^5,        # upper limit of zips, greater is invalid
                                solim = 0,            # upper limit of SONumber, greater is invalid,zero = no limit
                                dulim = 1500){        # upper limit of duration as days
  Result <- GetTime(GetTime(data,9),10) # get time series
  Result <- Convert(Result,c(4,5),class=zipclass,na.rm = na.rm,limupper = ziplim,split = TRUE) # filter zips
  Result <- Convert(Result,8,class=soclass,na.rm = na.rm,limupper = solim) # filter SOnumbers
  Result <- Convert(Result,c(7,11),na.rm = na.rm,limupper = dulim) # filter duration and one more
  Result <- Convert(Result,c(1,2,3,6),class = "character",na.rm = na.rm,limupper = solim) # set rest as char
  Result <- SetColNames(Result,type = "shipping.data") # set column names
  return(Result)
}

# Searching indexes using which() can cause problems if classes are not the same. Using this function would be
# more reliable.
Search <- function(x,vector){
  if(class(x)==class(vector)) # check if the classes are the same.
    return(which(vector == x))
  else{
    cat("Classes should be the same! ","(",class(x)," vs ",class(vector),")\n",sep = "")
    return(-1)
  }
}

# A quick solution for getting following data for each zip: location(city), lattitude, longtitude, amount of orders,
# state code, total sold units and total received payments.
# please note that postal code information should be given as characters without any remove of leading 
# zeros, total five digits.
LocationLevels <- function(data,zipcol){
  ReceipentZips <- Convert(data,zipcol,class = "character")[,zipcol] # trim white spaces and convert to character for searching
  ZipLevels <- levels(factor(unlist(ReceipentZips))) # determine how many different zips there are
  ZipLevels <- as.data.frame(matrix(ZipLevels,length(ZipLevels),1)) # save the zips as one column matrix
  
  Result <- as.data.frame(matrix(NA,nrow(ZipLevels),8)) # form a new data frame to later fill in
  Result[,2] <- ZipLevels
  colnames(Result) <- c("Location","ZipCode","Lat","Lon","Order Amount","StatesCode","TotalUnits",
                        "TotalPayments")
  
  data$TotalPayments <- data[,7]*data[,8] # sum these as total payments and make it nineth column
  cat("Initialization completed.\n")
  for (i in 1:nrow(Result)){ # get information from data
    indexes <- which(ReceipentZips == Result[i,2]) # use which() to locate all zips
    Result[i,c(5,7,8)] <- c(length(indexes),                     # Amount of orders
                            sum(as.numeric(data[indexes,7])),  # Amount of shipped units
                            sum(as.numeric(data[indexes,9])))  # Total price (should be multiplied by unit number)
    # progress bar as percentage
    if(i %% as.integer(nrow(Result)/100) == 0) cat("=",sep="")
  }
  cat("\nSummations completed.\n")
  
  if(!exists("Zips")) Zips <- GetZips() # if zip database not exists, import it.
  
  for(i in 1:nrow(Result)){ # match and get information from zip database
    if(is.na(Result[i,1]))
      Result[i,c(1,6,3,4)] <- Zips[match(ZipLevels[i,1],Zips[,1]),c(2,4,6,7)] # Match the data such as city, lat, lon
    # progress bar
    if(i %% as.integer(nrow(Result)/100) == 0) cat("=",sep="")
  }
  cat("\nMatchings completed.\n")
  return(Result) 
}

# Input of the following function is output of the function "LocationLevel()"
# return of this function can be directly plotted on map using plotly. result shows sale distribution by state
# returns an array with the following data for each state: amount of orders, total shipped units to that state 
# and gross profit according to payments of that state.
StateLevels <- function(data,statecol=6,unitscol=7,profitscol=8,na.rm=TRUE){
  temp <- levels(factor(unlist(data[,statecol]))) # get how many different states there are
  Result <- as.data.frame(matrix(NA,length(temp),5)) # declare and initialize a matrix to fill in later
  colnames(Result) <- c("StateCode","State","Orders","Units","GrossProfit")
  Result[,1] <- temp 
  
  for (i in 1:nrow(Result)){ # get information from data
    indexes <- which(data[,statecol] == Result[i,1]) # use which() to locate all states
    Result[i,c(3,4,5)] <- c(length(indexes),                  # Amount of orders
                            sum(as.numeric(data[indexes,unitscol])),  # Amount of shipped units
                            sum(as.numeric(data[indexes,profitscol])))  # Profit
  }
  
  if(!exists("Zips")) Zips <- GetZips() # if zip database does not exists, import it.
  
  for(i in 1:nrow(Result)){ # get information from zip database
    if(is.na(Result[i,2])) # according to state names
      Result[i,2] <- Zips[match(Result[i,1],Zips[,4]),3] # Match the data 
  }
  
  if(na.rm) Result <- na.omit(Result)
  
  Result$hover <- with(Result, paste(State, '<br>', 
                                     "Order Amount : ", Orders, "<br>",
                                     "Total Units : ", Units,  "<br>",
                                     "Gross Profit : ", round(GrossProfit,2)," USD",sep = ""))
  return(Result)
}

# calculates correlation coefficients of a given matrix, plots it on a table. if needed, scales graph for
# better observation
CorLevels <- function(data,
                      filename = "Correlations.pdf",
                      device   = "pdf",
                      main     = "Correlations",
                      scaling  = FALSE,
                      method   = "circle",
                      type     = "full"){
  
  Result <- cor(data) # calculate correlation coefficients
  Original <- Result
  n <- 0 # initialize a variable for counting
  if(scaling){
    while(range(abs(Result))[1]<0.1){ # if absolute value of the smallest value if less than 0.1
      for(i in 1:nrow(Result)){         # take square roots of the data to rescale, until it can be
        for(j in 1:nrow(Result)){       # easily observable
          if(Result[i,j] < 0)
            Result[i,j] <- (-1)*sqrt(abs(Result[i,j])) # sqrt the abs value and then multiply by minus one
          else
            Result[i,j] <- sqrt(Result[i,j]) # normally sqrt the value itself
        }
      }
      n <- n + 1 # count the number of cycles
    }
  }
  if(n>0){ # warn the user about scaling
    cat(n," times square root is taken (elementwise) for better scaling.\n",sep="")
    main <- paste(main,"\n(",n," times square root is taken elementwise for better scaling.)",sep="")
  }
  require(corrplot)
  switch (device,
          "pdf" = pdf(filename),# save following plot to a file with pdf format
          "png" = png(filename)
  )
  corrplot(Result,main = main,method = method,type = type,mar=c(0,0,3,0))
  dev.off() # turn the device off
  return(Original) # return the original correlations matrix
}

# # Following function is INCOMPLETE and NOT working. Should be reorganized.
# MultiPlotly3d <- function(data,
#                           x,
#                           y,
#                           z,
#                           color,
#                           fname = "3d",
#                           main = "3dPlot",
#                           type = "scatter3d", 
#                           mode = "markers"){
#   
#   require(plotly)
#   if(is.null(colnames(x)))
#     x <- as.data.frame(x) # convert to data frame if not already, (x maybe a multicolumn object too)
#   if(is.null(colnames(y)))
#     y <- as.data.frame(y) # convert to data frame if not already, (x maybe a multicolumn object too)
#   
#   # for(i in 1:ncol(z)){
#   filename <- paste(fname,"_",i,"_",
#                     colnames(x[1]),"_",
#                     colnames(y[1]),"_",
#                     colnames(z[i]),".html",sep = "") # form a file name with an index
#   
#   plotname <- paste(main," #",i,sep = "")
#   
#   p <- plot_ly(data, x = x, y = y, z = z[,i],
#                color = t, 
#                type = "scatter3d", 
#                mode = "markers") %>% 
#     layout(title = plotname,
#            scene = list(
#              xaxis = list(title = colnames(x[1])), 
#              yaxis = list(title = colnames(y[1])), 
#              zaxis = list(title = colnames(z[i]))))
#   htmlwidgets::saveWidget(as.widget(p), filename) # save as an html page to keep interactive tools
#   cat("Image",filename,"saved to",getwd(),"\n")
#   # }
#   return(p)
# }

# Check Graphical devices, warn the user if any of them is not working.
CheckDevices <- function(){
  temp <- capabilities()
  devs <- temp[which(temp==F)]
  if(length(devs)>0)
    cat("Warning: Devices",names(devs),"are not available.\n")
  else 
    cat("All devices are properly working.")
  return(temp)
}

# following function checks whether a given coordinate is inside USA borders, the reason it has a switch()
# function is easier future improvements for others countries.
is.inside <- function(lat,lon,map = "USA"){
  if(length(lat)==length(lon)){
    Result <- list()
    switch (map,
            "USA" = for(i in 1:length(lat))
              Result[i] <- 25 < lat[i] && lat[i] < 50 && -124 < lon[i] && lon[i] < -66
    )
  }
  else
    cat("Lengths of lattitude and longitude vectors must be the same.\n")
  return(Result)
}

# for a given vector or a list of source, search elements of source in target and list them
Search.List <- function(source,target,col){
  Result <- data.frame() # initialize a data frame to fill in later
  for(i in 1:length(source)){
    index <- which(target[,col]==source[i]) # search the source in target, get indexes
    if(length(index)!=0) # if found
      Result <- rbind(Result,cbind(rep(source[i],length(index)),target[index,])) #
  }
  return(Result)
}

# input is zip code. easier search of coordinates, this function returns just lat and lon
zip.coordinates <- function(x){
  if(!exists("Zips")) Zips <- GetZips() # if zip database does not exists, import it.
  Result <- as.data.frame(matrix(NA,length(x),2)) # to store lat and lon respectively
  for(i in 1:length(x))
    Result[i,] <- Zips[match(x[i],Zips[,1]),c(6,7)]
  colnames(Result) <- c("Lattitude","Longitude")
  return(Result)
}

# input is a zip code. prepare a function for easier city and state matching
zip.location <- function(x){
  if(!exists("Zips")) Zips <- GetZips() # if zip database does not exists, import it.
  Result <- as.data.frame(matrix(NA,length(x),2)) # to store lat and lon respectively
  for(i in 1:length(x))
    Result[i,] <- Zips[match(x[i],Zips[,1]),c(2,4)]
  colnames(Result) <- c("City","StateCode")
  return(Result)
}

# just input a list of product names (but as levels, each element should be unique in list) and let it search
# this function is focused on collecting the data of top items such as bestseller or most profitable, please note that
# it does not find bestseller or else, it just extracts corresponding info of the given items from raw sale data
TopItems.List <- function(products,     # top items name list e.g. bestseller 10. (must be a data frame)
                          saledata,     # raw sale data e.g. Month10.txt, Month11.txt, etc.
                          type="bestseller", # needed for final sorting
                          col.unit=7,   # column of unitsshipped in raw sale data
                          col.total=9,  # column of total profit, after binded to the raw sale data
                          col.price=8){ # column of prices in raw sale data
  
  Result <- data.frame(Name=products,Quantity=NA,Profit=NA,Price=NA,Orders=NA)
  for(i in 1:nrow(Result)){
    index <- which(saledata[,5] == Result[i,1])
    Result[i,c(2,3,4,5)]<- c(sum(saledata[index,col.unit]), # sold quantity
                             sum(saledata[index,col.total]), # total profit from that item
                             saledata[index[1],col.price],   # price of the item
                             length(index))          # number of orders
  }
  switch (type,
          "bestseller" = Result <- Sort(Result,2,decreasing = TRUE), # sort w.r.t. quantity
          "mostprofitable" = Result <- Sort(Result,3,decreasing = TRUE), # sort w.r.t. profit
          "mostordered" = Result <- Sort(Result,5,decreasing = TRUE) # sort w.r.t. number of orders
  )
  return(Result)
}

# list the best products according to 3 types and number of products, e.g. top 100, top 10 etc.
Product.List <- function(saledata,          # raw sale data e.g. Month10.txt etc.
                         type="bestseller", # bestseller, mostprofitable or mostordered
                         limit=15,         # intentionally taken 10 more. exclude unwanted.for top 100.
                         savetofile=FALSE,  # save to csv for backup and future recovers
                         filename="Product_List.csv", # filename for probable file write
                         samp.perc=20){ # percentage of sample taken initially
  
  saledata$Total <- saledata$UnitsShipped*saledata$AverageUnitPrice # get the profit from that product
  # take a 20% sample
  # aim is to pull out significant names from sample, since focus is top products
  sample.saledata <- saledata[sample(nrow(saledata),round((nrow(saledata)/100)*samp.perc)),]
  # be careful, both data must be white-space trimmed, even the lists must be. 
  products <- data.frame(Name=as.character(levels(factor(sample.saledata[,5])))) # see and store how many different products
  switch (type,
          "bestseller" = {
            # ad hoc quantity values, just for faster listing
            products$Quantity <- sapply(products[,1],
                                        function(x) sum(sample.saledata[which(sample.saledata[,5] == x),7])) # rowwise
            Result <- TopItems.List(products = Sort(products,2,decreasing = TRUE)[1:limit,1],
                                    saledata = saledata,type = "bestseller")
          },
          "mostprofitable" = {
            products$Profit <- sapply(products[,1],
                                      function(x) sum(sample.saledata[which(sample.saledata[,5] == x),9])) # rowwise
            Result <- TopItems.List(products = Sort(products,2,decreasing = TRUE)[1:limit,1],
                                    saledata = saledata,type = "mostprofitable")
          },
          "mostordered" = {
            products[,2] <- sapply(products[,1],
                                   function(x) length(which(sample.saledata[,5] == x)))
            Result <- TopItems.List(products = Sort(products,2,decreasing = TRUE)[1:limit,1],
                                    saledata = saledata,type = "mostordered")
          }
  )
  # save to a csv file as backup
  if(savetofile){
    write.csv(Result,file = filename,row.names = FALSE) # no row.names to prevent possible reading errors
    cat("File",filename,"saved to",getwd(),"\n")
  }
  return(Result)
}

# for matching the data from raw sale data of bestseller or most profitable item etc. 
Partial.SaleData <- function(product,          # searched product name, can be a vector
                             saledata,         # raw sale data e.g. Month10.txt, Month11.txt etc.
                             col.itemname=5,   # column that stores product names
                             savetofile=FALSE, # save to csv for backup and future recovers
                             filename="Partial_SaleData.csv"){ # filename for probable file write
  
  Result <- Search.List(product,saledata,col.itemname)[,-1]
  # save to a csv file as backup
  if(savetofile){
    write.csv(Result,file = filename,row.names = FALSE) # no row.names to prevent possible reading errors
    cat("File",filename,"saved to",getwd(),"\n")
  }
  
  return(Result)
}

# for easier obtaining of a product's shipping data (NOT tested)
Partial.ShipData <- function(sonumber,           # sonumbers of SALEDATA (output of Partial.SaleData) NOT Rawdata
                             product,            # name of the product, just one name, not a set of names
                             ship,               # raw ship data, i.e. ShippingData_Mont...txt
                             col.sonumber=8,     # number of the column containing sonumbers in raw ship data
                             furtherinfo=FALSE,  # show detailed info 
                             savetofile=FALSE,   # save to csv for backup and future recovers
                             filename="Partial_ShipData.csv"){ # filename for probable file write
  
  mylevels <- as.character(levels(factor(sonumber))) # see levels of SONumbers
  
  # convert to characters for faster searching, then find indexes that contain searched SONumber
  ShipSONumber <- as.character(ship[,col.sonumber])
  index <- list(a=c(1,2,3),b=c(1,2)) # arbitrarily initialize, let it store rows with various lengths
  for(i in 1:length(mylevels))
    index[[i]] <- which(mylevels[i]==ShipSONumber)
  print("Indexes determined.")
  
  # get corresponding shipping data from indexes
  temp <- data.frame() # declare a temporary data frame to use in for loop (this make algorithm simplar)
  Result <- as.data.frame(matrix(NA,nrow=sum(lengths(index,use.names = FALSE)),
                                 ncol=21)) # preallocate main data frame
  colnames(Result) <- c("TrackingNumber","Company","ShippingCode","SenderZip","ReceipentZip","Type",
                        "ShippingCost","SONumber","DateShipped","DateDelivered","Duration","S.Lat",
                        "S.Lon","R.Lat","R.Lon","S.City","S.StateCode","R.City","R.StateCode",
                        "Distance","Product")
  
  # match data
  for(i in 1:length(index))
    temp <- rbind(temp,ship[index[[i]],])
  
  Result[,1:11] <- temp # assign it to main data frame
  print("Base data is extracted from raw shipping data.")
  
  # get coordinates as lat and lon from zips
  Result[,c(12,13)] <- zip.coordinates(Result[,4])[,c(1,2)] # find coordinates of sender zips
  Result[,c(14,15)] <- zip.coordinates(Result[,5])[,c(1,2)] # find coordinates of receipent zips
  
  # get location names from zips
  Result[,c(16,17)] <- zip.location(Result[,4])[,c(1,2)] # find city/state of sender zips
  Result[,c(18,19)] <- zip.location(Result[,5])[,c(1,2)] # find city/state of receipent zips
  
  # calculate the distance between supplier and customer
  require(geosphere)
  Dist <- distHaversine(cbind(as.numeric(Result$S.Lon),as.numeric(Result$S.Lat)),
                        cbind(as.numeric(Result$R.Lon),as.numeric(Result$R.Lat))) # in meters
  Result$Distance <- round(Dist/1000,3) # as kilometers
  Result$Product <- product # add product name to prevent confusions
  print("Location information is collected.")
  
  # save to a csv file as backup
  if(savetofile){
    write.csv(Result,file = filename,row.names = FALSE) # no row.names to prevent possible reading errors
    cat("File",filename,"saved to",getwd(),"\n")
  }
  
  # further information for BestSeller item's Shipping Data
  if(furtherinfo){
    paste("\nFurther information:")
    paste("There are",levels(factor(Result$Type)),"different shipment types.") # different shipment types
    paste("Average delivery duration is",mean(na.omit(Result$Duration)),"days.") # average delivery day
    paste("Average cost for each delivery is",mean(na.omit(Result$ShippingCost)),"USD.") # average cost
  }
  return(Result)
}

# for easier obtaining of a product's shipping data (NOT tested)
Partial.ShipData.List <- function(saledata,           # output of Partial.ShipData(), can include various product names
                                  ship,               # raw ship data, i.e. ShippingData_Mont...txt
                                  sale.sonumber=1,    # column index of SONumbers in saledata
                                  sale.product=5,     # column index of product names in saledata
                                  col.sonumber=8,     # number of the column containing sonumbers in raw ship data
                                  furtherinfo=FALSE,  # show detailed info 
                                  savetofile=FALSE,   # save to csv for backup and future recovers
                                  filename="Partial_ShipData.csv"){ # filename for probable file write
  require(geosphere)
  if(!exists("Zips")) Zips <- GetZips() # if zip database does not exists, import it.
  Output <- as.data.frame(matrix(NA,nrow=sum(lengths(index,use.names = FALSE)),
                                 ncol=21)) # preallocate main data frame, this will be final output
  colnames(Output) <- c("TrackingNumber","Company","ShippingCode","SenderZip","ReceipentZip","Type",
                        "ShippingCost","SONumber","DateShipped","DateDelivered","Duration","S.Lat",
                        "S.Lon","R.Lat","R.Lon","S.City","S.StateCode","R.City","R.StateCode",
                        "Distance","Product")
  
  prods <- list(a=c(1,2,3),b=c(1,2)) # just initialize a list. needed for a list with varying row lengths.
  prodlevels <- as.character(levels(factor(saledata[,sale.product]))) # levels of names
  SaleSONumber <- as.character(saledata[,sale.product])
  for(i in 1:length(prodlevels)) # find indexes, to form corresponding "saledata" for each product
    prods[[i]] <- which(SaleSONumber==prodlevels[i]) # store indexes
  
  # perform data collection for each item, rbind() them respectively, main loop.
  # note that one loop corresponds to one item
  for(j in 1:length(prods)){
    mylevels <- as.character(levels(factor(saledata[prods[[j]],sale.sonumber]))) # store levels of SONumbers
    product <- prodlevels[j]
    # convert to characters for faster searching, then find indexes that contain searched SONumber
    ShipSONumber <- as.character(ship[,col.sonumber])
    index <- list(a=c(1,2,3),b=c(1,2)) # arbitrarily initialize, let it store rows with various lengths
    for(i in 1:length(mylevels))
      index[[i]] <- which(mylevels[i]==ShipSONumber) # each element corresponds to a row of result matrix
    print("Indexes determined.")
    
    # get corresponding shipping data from indexes
    temp <- data.frame() # declare a temporary data frame to use in for loop (this make algorithm simplar)
    Result <- as.data.frame(matrix(NA,nrow=sum(lengths(index,use.names = FALSE)),
                                   ncol=21)) # preallocate main data frame
    colnames(Result) <- c("TrackingNumber","Company","ShippingCode","SenderZip","ReceipentZip","Type",
                          "ShippingCost","SONumber","DateShipped","DateDelivered","Duration","S.Lat",
                          "S.Lon","R.Lat","R.Lon","S.City","S.StateCode","R.City","R.StateCode",
                          "Distance","Product")
    
    # match data
    for(i in 1:length(index))
      temp <- rbind(temp,ship[index[[i]],])
    
    Result[,1:11] <- temp # assign it to main data frame, for first 11 columns
    print("Base data is extracted from raw shipping data.")
    
    # get coordinates as lat and lon from zips
    Result[,12:20] <- LocationData(Result)
    # Result[,c(12,13)] <- zip.coordinates(Result[,4])[,c(1,2)] # find coordinates of sender zips
    # Result[,c(14,15)] <- zip.coordinates(Result[,5])[,c(1,2)] # find coordinates of receipent zips
    # 
    # # get location names from zips
    # Result[,c(16,17)] <- zip.location(Result[,4])[,c(1,2)] # find city/state of sender zips
    # Result[,c(18,19)] <- zip.location(Result[,5])[,c(1,2)] # find city/state of receipent zips
    # 
    # # calculate the distance between supplier and customer
    # Dist <- distHaversine(cbind(as.numeric(Result$S.Lon),as.numeric(Result$S.Lat)),
    #                       cbind(as.numeric(Result$R.Lon),as.numeric(Result$R.Lat))) # in meters
    # Result$Distance <- round(Dist/1000,3) # as kilometers
    Result$Product <- product # add product name to prevent confusion
    print("Location information is collected.")
    
    Output <- rbind(Output,Result)
  }
  
  # save to a csv file as backup
  if(savetofile){
    write.csv(Output,file = filename,row.names = FALSE) # no row.names to prevent possible reading errors
    cat("File",filename,"saved to",getwd(),"\n")
  }
  
  # further information for BestSeller item's Shipping Data
  if(furtherinfo){
    paste("\nFurther information:")
    paste("There are",levels(factor(Output$Type)),"different shipment types.") # different shipment types
    paste("Average delivery duration is",mean(na.omit(Output$Duration)),"days.") # average delivery day
    paste("Average cost for each delivery is",mean(na.omit(Output$ShippingCost)),"USD.") # average cost
  }
  return(Output)
}

# find lat,lon,city,state and distance values for a given set of zip codes
# initially, the content of the function was a part of partial.shipdata() function but later
# it was extracted to make available for external uses.
LocationData <- function(data,       # can be raw shipdata or saledata, or any type of data containing zip codes
                         col.sen=4,  # column number of sender zips
                         col.rec=5){ # column number of receipent zips
  
  require(geosphere)
  if(!exists("Zips")) Zips <- GetZips() # if zip database does not exists, import it.
  Result <- as.data.frame(matrix(NA,nrow(data),9)) # preallocate output matrix
  colnames(Result) <- c("S.Lat","S.Lon","R.Lat","R.Lon","S.City","S.StateCode","R.City","R.StateCode",
                        "Distance")
  # get coordinates as lat and lon from zips
  Result[,c(1,2)] <- zip.coordinates(data[,col.sen])[,c(1,2)] # find coordinates of sender zips
  Result[,c(3,4)] <- zip.coordinates(data[,col.rec])[,c(1,2)] # find coordinates of receipent zips
  
  # get location names from zips
  Result[,c(5,6)] <- zip.location(data[,col.sen])[,c(1,2)] # find city/state of sender zips
  Result[,c(7,8)] <- zip.location(data[,col.rec])[,c(1,2)] # find city/state of receipent zips
  
  # calculate the distance between supplier and customer
  Dist <- distHaversine(cbind(as.numeric(Result$S.Lon),as.numeric(Result$S.Lat)),
                        cbind(as.numeric(Result$R.Lon),as.numeric(Result$R.Lat))) # in meters
  Result[,9] <- round(Dist/1000,3) # as kilometers
  return(Result)
}

# collect and organize data of shipping types, for one product
CargoTypes <- function(product,            # name of the product
                       shipdata,           # input shipping data output of Partial.Shipdata()
                       col.type=6,         # number of the column which contains shipment types in shipdata
                       col.cost=7,         # column number of the shipping costs in shipdata
                       col.duration=11,    # in shipdata
                       col.distance=20,    # in shipdata
                       savetofile=FALSE,   # save to csv for backup and future recovers
                       filename="Cargo_Types.csv"){ # filename for probable file write
  
  Result <- data.frame(Product=product,
                       Type=levels(factor(shipdata[,col.type])),
                       AverageCost=NA,
                       TotalUse=NA,
                       AverageDuration=NA,
                       TotalCost=NA,
                       AverageDistance=NA)
  
  Result[,3] <- sapply(Result[,2], function(x) mean(shipdata[which(shipdata[,col.type]==x),col.cost])) # average cost
  Result[,4] <- sapply(Result[,2], function(x) length(which(shipdata[,col.type]==x))) # total uses
  Result[,5] <- sapply(Result[,2], function(x) mean(na.omit(shipdata[which(shipdata[,col.type]==x),col.duration]))) # ave. dur.
  Result[,6] <- sapply(Result[,2], function(x) sum(shipdata[which(shipdata[,col.type]==x),col.cost])) # total cost
  Result[,7] <- sapply(Result[,2], function(x) mean(na.omit(shipdata[which(shipdata[,col.type]==x),col.distance]))) # ave. dist.
  Result <- Sort(Result,3,decreasing = TRUE)
  
  # save to a csv file as backup
  if(savetofile){
    write.csv(Result,file = filename,row.names = FALSE) # no row.names to prevent possible reading errors
    cat("File",filename,"saved to",getwd(),"\n")
  }
  
  return(Result)
}

# collect and organize provider's data
Warehouses <- function(senderzips,       # sender zips of saledata (output of Partial.SaleData())
                       saledata,         # input sale data (NOT Raw sale data)
                       shipdata,         # input shipping data (NOT raw shipping data)
                       col.zip=4,        # column number of sender zips in SHIPDATA
                       col.zip.sale=3,   # column number of sender zips in SALEDATA
                       col.unit=7,       # col number of units in SALEDATA
                       col.distance=20,  # col number in shipdata
                       col.duration=11,  # in shipdata
                       col.cost=7,       # in shipdata
                       r.lat=14,         # in shipdata
                       r.lon=15,         # in shipdata
                       savetofile=FALSE, # save to csv for backup and future recovers
                       filename="Warehouses_Data.csv"){ # filename for probable file write
  
  if(!exists("Zips")) Zips <- GetZips() # if zip database not exists, import it.
  
  Result <- Search.List(as.character(levels(factor(senderzips))),Zips,1)[,-1]
  Result <- cbind(Result,Uses=NA,Units=NA,Ave.Distance=NA,Ave.Duration=NA,Ave.Cost=NA,Ave.R.Lat=NA,Ave.R.Lon=NA)
  shipdata <- Convert(shipdata,c(12,13,14,15))
  Result <- Convert(Result,c(6,7))
  
  for(i in 1:nrow(Result)){
    index <- which(shipdata[,col.zip]==Result[i,1])
    Result[i,c(8,9,10,11,12,13,14)] <- c(length(index), # uses
                                         sum(saledata[which(saledata[,col.zip.sale]==Result[i,1]),col.unit]), # units 
                                         mean(na.omit(shipdata[index,col.distance])), # average distance
                                         mean(na.omit(shipdata[index,col.duration])), # average duration
                                         mean(na.omit(shipdata[index,col.cost])),     # average cost
                                         mean(na.omit(shipdata[which(shipdata[,r.lat]==Result[i,6]),r.lat])),
                                         mean(na.omit(shipdata[which(shipdata[,r.lon]==Result[i,7]),r.lon]))
    )
  }
  
  Result <- Sort(Result,9,decreasing = TRUE)
  
  # save to a csv file as backup
  if(savetofile){
    write.csv(Result,file = filename,row.names = FALSE) # no row.names to prevent possible reading errors
    cat("File",filename,"saved to",getwd(),"\n")
  }
  
  return(Result)
}

# match two given data set
Match.rows <- function(source,col.sou,target,col.tar){
  Result <- data.frame() # initialize a data frame to fill in later
  for(i in 1:length(source[,col.sou])){
    index <- which(target[,col.tar]==source[i,col.sou]) # search the source in target, get indexes
    len <- length(index) # store how many times it is encountered
    if(len!=0){ # if found
      for(j in 1:len)
        Result <- rbind(Result,cbind(source[i,],target[index[[j]],])) # write corresponding matches
      target <- target[-index,] # exclude elements that has been listed, (on test)
    }
    else if(len==0){ # if could not be found
      temp <- matrix(NA,1,ncol(target))
      colnames(temp) <- colnames(target)
      Result <- rbind(Result,cbind(source[i,],temp)) # fill with NA's
    }
  }
  return(Result)
}

# this function is needed for filtering the shipping data. it gathers information about shipping types and one can decide
# which type of shipping should be excluded.
levels.ship <- function(data,col.type=6,col.cost=7,col.duration=11,distances=FALSE){
  Result <- as.data.frame(levels(factor(data[,col.type])))
  Result$AveCost <- sapply(Result[,1],function(x) mean(na.omit(data[which(data[,col.type]==x),col.cost])))
  Result$TotalCost <- sapply(Result[,1],function(x) sum(na.omit(data[which(data[,col.type]==x),col.cost])))
  Result$AveDuration <- sapply(Result[,1],function(x) mean(na.omit(data[which(data[,col.type]==x),col.duration])))
  Result$Uses <- sapply(Result[,1],function(x) length(which(data$Type==x)))
  if(distances){
    Result$Distance <- sapply(Result[,1],function(x) mean(na.omit(data[which(data[,col.type] == x),20])))
    Result$Index <- Result$Distance/(Result$AveCost*Result$AveDuration)
    colnames(Result) <- c("Type","AveCost","TotalCost","AveDuration","Uses","Distance","Index")
  }
  else{
    colnames(Result) <- c("Type","AveCost","TotalCost","AveDuration","Uses") 
  }
  Result <- Sort(Result,2,decreasing = TRUE)
  return(Result)
}



# this function is for filtering and gathering location information about shipping data
Filter.ShippingData <- function(Ship, # input raw shipping data after it is formatted with Format.ShipData()
                                location.info=TRUE, # in order to collect location lat/lon info about a row
                                file.write=TRUE, # true if you want to save to csv file
                                file.name="Shipping_Filtered.csv" # name of the file to be written
){
  # Filter
  Result <- Ship[-which(Ship$ShippingCost==0),] # filter by cost, exclude transactions with no cost
  Result <- Result[-which(Result$Type == "UPS Ground"),] # filter by type, exclude UPS Ground shipping
  
  # Further filtrate according to shipping types
  typeinfo <- levels.ship(Result,distances = FALSE) # get shipping type information
  print(typeinfo)
  
  # determine to pull out
  print("According to shipping types, take the corresponding data of rows :")
  from <- as.integer(readline(prompt="from : ")) # from which row
  to <- as.integer(readline(prompt="to : ")) # to which row
  Result <- Result[which(Result$Type %in% typeinfo[from:to,1]),] # filter by type
  
  # Collect data for locations
  if(location.info)
    Result <- cbind(Result,LocationData(Result)) # column-bind them together
  
  # Export to csv file
  if(file.write){
    write.csv(Result,file.name,row.names = FALSE)
    cat("File",file.name,"is saved to",getwd(),"\n")
  }
  
  return(Result)
}
  
### FOLLOWING FUNCTION(S) SHOULD BE TESTED! ###  
  # input ultimate filtered and location data included shipping data, for ONE quarter, and input number of the month and 
  # raw sale data, this function will match shipping data with sale data, and create a one merged data.
  Match.ShipData <- function(shipfiltered, # ultimate filtered and location data included shipping data 20 columns
                             month, # input a month number as integer
                             saledata, # raw sale data e.g. Month10.txt, Month11.txt etc.
                             iterations = 10, # number of part to divide into when computing
                             filename = "Most_Expensive_Matched_M11_", # file name to be saved
                             begin = 1, # beginning of the for loop, could be continued from other steps
                             ship.sonumber = 8, # sonumber column in filtered shipping data
                             sale.sonumber = 1 # sonumber column in filtered sale data
  ){
    require("lubridate")
    # create a directory for tidier work
    main_path <- getwd() # "C:/Users/USER/Desktop/R"
    current_path <- paste(getwd(),paste("DataMatch",sample(100,1),sep="_"),sep = "/") # folder names
    dir.create(current_path) # create a new directory to store files
    setwd(current_path) # set new working directory
    
    # pull out just one month, and sort according to shipping cost
    temp_ship <- Sort(shipfiltered[which(month(shipfiltered$DateShipped)==as.integer(month)),],7,decreasing = TRUE)
    temp_raw <- Search.List(temp_ship[,ship.sonumber],saledata,ship.sonumber)[,-1] # get corresponding rows of saledata, by sonumber
    rows <- nrow(temp_ship) # get number of rows to use in following for loop
    ## divide into fractions and then unify into one big file, for Month10.txt
    for(i in begin:iterations){
      cat("Step",i,"\n")
      index <- seq((i-1)*(rows/iterations)+1,i*(rows/iterations)) # determine interval of indexes
      test_match <- Match.rows(temp_ship[index,],ship.sonumber,temp_raw,ship.sonumber) # match data and bind together as a data frame
      check_sen <- test_match[,4]==test_match[,23] & test_match[,5]==test_match[,24] # check receipent and sender zips whether they match
      test_match <- test_match[check_sen,] # take only who match by zips
      write.csv(test_match,paste(filename,i,".csv",sep = ""),row.names = FALSE) # write to file with order
      cat("File",paste(filename,i,".csv",sep = ""),"is saved to",getwd(),"\n") # inform user
    }
    
    Result <- data.frame()
    for(i in 1:iterations)
      Result <- rbind(Result,read.csv(paste(filename,i,".csv",sep = ""),row.names = NULL))
    
    setwd(main_path) # reset working directory
    write.csv(Result,paste(filename,".csv",sep = ""),row.names = FALSE)
    cat("File",paste(filename,".csv",sep = ""),"is saved to",getwd(),"\n") # inform user
    
    return(Result)
  }
### ABOVE 2 FUNCTIONS SHOULD BE TESTED! ###
  

### ----------------------------------------------------------------------- ###
### - Text File and Data Editor Functions --------------------------------- ###
### ----------------------------------------------------------------------- ###

# Kindly note that following functions are not optimized, but left original as first written.
TxtFindFirst <- function(filename,value,delimiter){ # return the indexes of the value found at first
  data <- read.table(filename,sep=delimiter) # read the text file
  for (j in 1:ncol(data)){
    for (i in 1:nrow(data)){
      if(data[i,j] == value){# if found, store indexes
        Result <- c(i,j)
        names(Result) <- c("Row","Column")
        return(Result)
      } 
    }
  }
  cat(value,"is not found!\n")
  return(-1)
}

TxtFindAll <- function(filename,value,delimiter){ # return the indexes of the value found everywhere
  data <- read.table(filename,sep=delimiter) # read the text file
  n <- 1 # initialize a variable for indexing
  rows <- 0 # declare this in order to store row indexes
  cols <- 0 # declare this in order to store column indexes
  for (j in 1:ncol(data)){
    for (i in 1:nrow(data)){
      if(data[i,j] == value){ # if found, store indexes
        rows[n] <- i
        cols[n] <- j
        n <- n + 1 # increase by one to store possible incoming indexes, in "rows" and "cols"
      }
    }
  }
  if(n==1){
    cat(value,"is not found!\n")
    return(-1)
  }
  cat(value,"found at\n")
  Result <- c(rows,cols) # bound together as one dimensional vector
  dim(Result) <- c(n-1,2) # to give the final shape
  colnames(Result) <- c("Row","Column") # assign column names
  return(Result)
}

TxtEditRow <- function(filename,rowindex,newvalue,delimiter){ # edit a complete row, return edited data
  data <- read.table(filename,sep=delimiter) # read the text file
  if(ncol(data) != length(newvalue)){ # do nothing if incorrect dimensions
    cat("It should have",ncol(data),"columns.")
    return (-1)
  }
  else{
    Result <- data # back up the content
    temp1 <- c("was : ",Result[rowindex,]) # define a temporary variable for better display
    dim(temp1) <- c(1,ncol(Result)+1)
    print(temp1)
    Result[rowindex,] <- newvalue # set new values
    temp2 <- c("now : " ,Result[rowindex,])
    dim(temp2) <- c(1,ncol(Result)+1)
    print(temp2)
    return(Result)
  }
}

TxtSort <- function(filename,colindex,delimiter){ # descending order, with respect to chosen column
  data <- read.table(filename,sep=delimiter) # read the text file
  Result <- data # back up the content
  rows <- nrow(data) # store the number of elements in that column
  # Bubble sort algorithm starts
  for (i in 1:rows){
    for (n in 1:(rows-1)){ # inspect numbers by pairs
      if(!is.null(Result[n,colindex]) & !is.null(Result[n+1,colindex])){ # if not NULL
        if(Result[n,colindex] < Result[n+1,colindex]){ # compare each pair inside
          temp <- Result[n,]          #
          Result[n,] <- Result[n+1,]  # relocate entire row
          Result[n+1,] <- temp        #
        }
      }
      else # if NULL
        cat("NULL at :",n,colindex)
    }  
  }
  # Bubble sort algorithm ends
  return (Result)
}

TxtConcatenate <- function(filename1,delimiter1,filename2,delimiter2){
  data1 <- read.table(filename1,sep = delimiter1) # read the text file
  data2 <- read.table(filename2,sep = delimiter2)
  if (ncol(data1) != ncol(data2)){ # do nothing if incorrect dimensions
    print("Dimensions must agree!")
    return (-1)
  }
  Result <- matrix(0,nrow(data1)+nrow(data2),ncol(data1)) # initialize with zeros
  Result <- data1 # fill data1 from the beginning
  for (i in (nrow(data1)+1):(nrow(data1)+nrow(data2))) # fill data2 to the rest
    Result[i,] <- data2[i-nrow(data1),]
  return (Result)
}
