library(rnoaa)  #get rnoaa from your R library to run
library(devtools)

setwd("~/PycharmProjects/weather-forecast/actual data gathering")
options(noaakey = "bhDBsvcWmEFSWJDwJFddstlxtunZkYKp")

start_date = "2017-09-30"
end_date = "2017-12-09"

datatypes = c('PRCP', # rain
              'SNOW',  # snow
              'TMIN',  # min temp (tenths of degrees C)
              'TMAX',  # max temp (tenths of degrees C)
              'TAVG',  # average temperature (tenths of degrees C)
              'AWDR',  # average wind degree
              'AWND')  # average wind speed(tenths of meters per second)

stationsids <- as.data.frame(matrix(0, ncol = 2, nrow = 53))
colnames(stationsids) = c('id', 'name')
stationsids$id = c(
  'USW00023174',
  #LA
  'USW00094728',
  #NEW YORK
  'USW00003145',
  #YUMA
  'USW00023160',
  'USW00023044',
  'USW00003032',
  'USW00012970',
  'USW00012917',
  'USW00012916',
  # NEW ORLEANS AIRPORT NOT HOUMA
  'USW00013899',
  'USW00093845',
  'USW00012839',
  'USW00053867',
  'USW00003888',
  'USW00093806',
  'USC00228445',
  # stoneville not hollandale
  'USW00013963',
  'USW00003954',
  'USW00023047',
  'USW00023050',
  'USW00003103',
  'USW00053123',
  'USW00093193',
  'USW00023271',
  'USW00024128',
  # WINNEMUCCA AIRPORT
  'USW00024127',
  'USW00023066',
  'USW00093067',
  'USC00143527',
  'USW00013988',
  #kansas
  'USW00003960',
  'USW00093821',
  'USW00013866',
  'USW00013897',
  #Nashville
  'USW00013891',
  'USW00013722',
  'USW00093738',
  'USW00004853',
  'USW00014768',
  'USW00014606',
  'USW00014819',
  'USW00014922',
  'USW00014944',
  'USC00396948',
  'USW00024027',
  'USW00024131',
  'USW00024128',
  #WINNEMUCCA AIRPORT not Denio
  'USW00024229',
  'USW00094176',
  'USW00024143',
  #GREAT FALLS AIRPORT not Chateau
  'USC00247560',
  'USW00024011',
  'USW00094967'  #PARK RAPIDS MUNICIPAL AIRPORT not Bemidji
)

stationsids$name = c(
  'Los Angeles',
  'New York City',
  'Yuma',
  'Tucson',
  'El Paso',
  'Sanderson',
  'San Antonio',
  'Beaumont',
  'Houma',
  'Pensacola',
  'Valdosta',
  'Miami',
  'Columbia',
  'Atlanta',
  'Tuscaloosa',
  'Hollandale',
  'Little Rock',
  'Oklahoma City',
  'Amarillo',
  'Albuquerque',
  'Flagstaff',
  'Las Vegas',
  'Fresno',
  'Sacramento',
  'Winnemucca',
  'Salt Lake City',
  'Grand Junction',
  'Denver',
  'Hays',
  'Kansas City',
  'St Louis',
  'Louisville',
  'Charleston',
  'Nashville',
  'Knoxville',
  'Raleigh',
  'Dulles',
  'Cleveland',
  'Rochester',
  'Bangor',
  'Chicago',
  'Minneapolis',
  'Sioux Falls',
  'Rapid city',
  'Rock Springs',
  'Boise',
  'Denio',
  'Portland',
  'Spokane',
  'Choteau',
  'Sidney',
  'Bismarck',
  'Bemidji'
)

output <- as.data.frame(matrix(0, ncol = 9, nrow = 0))
colnames(output) = c('Location',
                     'TMAX',
                     'TMIN',
                     'TAVG',
                     'AWND',
                     'AWDR',
                     'PRCP',
                     'SNOW',
                     'Date')
empty=0
cnt=0
for (i in 1:length(stationsids$id)) {
  out <-
    ncdc(
      datasetid = 'GHCND',
      datatypeid = datatypes,
      stationid = paste('GHCND:', stationsids[i, ]$id, sep = ''),
      startdate = start_date,
      enddate = end_date,
      limit = 1000
    )
  if(length(out$data) == 0){
    empty[cnt] = i
    cnt=cnt+1
    next
  }
  out <- subset(out$data, select = c(date, datatype, value))
  out$date <- gsub("\\T.*", "", out$date)
  while (length(out[, 1]) > 0) {
    temp_entries = subset(out, out$date == out$date[1])
    out = subset(out, out$date != temp_entries$date[1])
    
    temp_output <- as.data.frame(matrix(0, ncol = 9, nrow = 1))
    colnames(temp_output) = c('Location',
                              'TMAX',
                              'TMIN',
                              'TAVG',
                              'AWND',
                              'AWDR',
                              'PRCP',
                              'SNOW',
                              'Date')
    
    temp_output$Location = stationsids[i, ]$name
    temp_output$Date = temp_entries$date[1]
    x = subset(temp_entries$value, temp_entries$datatype == 'TMAX')
    if (!identical(x, integer(0))) {
      temp_output$TMAX = x
    } else{
      next
    }
    x = subset(temp_entries$value, temp_entries$datatype == 'TMIN')
    if (!identical(x, integer(0))) {
      temp_output$TMIN = x
    } else{
      next
    }
    x = subset(temp_entries$value, temp_entries$datatype == 'TAVG')
    if (!identical(x, integer(0))) {
      temp_output$TAVG = x
    } else{
      temp_output$TAVG = ''
    }
    x = subset(temp_entries$value, temp_entries$datatype == 'AWND')
    if (!identical(x, integer(0))) {
      temp_output$AWND = x
    } else{
      temp_output$AWND = ''
    }
    x = subset(temp_entries$value, temp_entries$datatype == 'AWDR')
    if (!identical(x, integer(0))) {
      temp_output$AWDR = x
    } else{
      temp_output$AWDR = ''
    }
    x = subset(temp_entries$value, temp_entries$datatype == 'PRCP')
    if (!identical(x, integer(0))) {
      temp_output$PRCP = x
    } else{
      temp_output$PRCP = ''
    }
    x = subset(temp_entries$value, temp_entries$datatype == 'SNOW')
    if (!identical(x, integer(0))) {
      temp_output$SNOW = x
    } else{
      temp_output$SNOW = ''
    }
    
    output = rbind(output, temp_output)
  }
}

write.csv(output,file = paste(start_date,'_',end_date,'_actual_data','.csv',sep = ""))
