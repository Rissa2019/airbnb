#### Cleaning data ###

### Load data
data = read.csv("listings_summary.csv", encoding = "UTF-8")
data_reviews = read.csv("reviews_summary.csv", encoding = "UTF-8")

### Select columns to keep
my_cols = c("id", "listing_url", "name", "description", "neighborhood_overview", "notes", "transit", "host_id", "host_name", "host_url", "host_response_time", "neighbourhood_group_cleansed", "city", "state", "country", "latitude", "longitude", "property_type", "room_type", "accommodates", "bathrooms", "bedrooms", "bed_type", "price")
new_data = data[my_cols]

### Rename columns to match ER diagram
col_names <- c("list_id", "list_url", "name", "description", "neighborhood_overview", "notes", "transit", "host_id", "host_name", "host_url", "response", "neighbourhood_goup", "city", "state", "country", "latitude", "longitude", "property_type", "room_type", "accommodates", "bathrooms", "bedrooms", "bed_type", "price")
names(new_data)<-col_names

### Change factors to NA
new_data[new_data=="N/A"]  <- NA 
new_data[new_data==""]  <- NA 

### Clean Cities
new_data$city<-"Berlin"

### Clean data_reviews
data_reviews[data_reviews==""]  <- NA 
data_reviews[data_reviews=="'"]  <- NA

### Save new dataset
write.csv(new_data, file = "new_data.csv")
write.csv(data_reviews, file = "reviews.csv")
