library(BIOMASS)

args <- commandArgs(trailingOnly = TRUE)
INPUT_FPATH <- args[1]
OUTPUT_FPATH <- args[2]


# Read the CSV file
data <- read.csv(INPUT_FPATH)

# Get the genus from the species name
Taxo <- correctTaxo(genus = data$scientific_name)  # Assuming 'species_name' contains genus
data$corrected_genus <- Taxo$genusCorrected

# Get the wood density
dataWD <- getWoodDensity(
  genus = data$corrected_genus,
  species = data$scientific_name,
  stand = data$unique_id,
  family = data$family_name,
)

# Creta wood density column
data$wood_density <- dataWD$meanWD

#Export file
write.csv(data,OUTPUT_FPATH, row.names = FALSE)
