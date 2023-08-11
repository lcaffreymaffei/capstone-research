library(tidyverse)
library(readxl)
library(writexl)
library(haven)

crosswalk <- read_dta("Dropbox/segregation lab/data and programs/data/ccd/schid crosswalks/SEDA-Crosswalk.dta")

gis <- read_excel("~/Dropbox/segregation lab/SABS/Data/1. Raw Data/SABS SY2015-2016/SABS_1516.xlsx", col_types = "text")

# MAKE VARIABLE NAMES ALL LOWERCASE 
names(gis) <- tolower(names(gis))

# FIX LEA IDS WHERE LEADING 0s WERE REMOVED IN UPLOAD
gis <- gis |>
  mutate(leaid = if_else(str_length(leaid) == 6, paste0("0", leaid), leaid))

# CHANGE LOWEST GRADE (GSLO) AND HIGHEST GRADE (GSHI) TO NUMERIC VALUES 
gis <- gis |>
  # kindergarten has value 0 for gslo
  mutate(gslo = if_else(gslo == "KG", "0", gslo),
         # preschool has value -1 for gslo
         gslo = if_else(gslo == "PK", "-1", gslo),
         # replace ungraded with NA for gslo
         gslo = na_if(gslo, "UG"),
         # replace N with NA for gslo
         gslo = na_if(gslo, "N"),
         # change gslo from character to numeric variable
         gslo = as.numeric(gslo),
         # kindergarten has value 0 for gshi
         gshi = if_else(gshi == "KG", "0", gshi),
         # preschool has value -1 for gshi
         gshi = if_else(gshi == "PK", "-1", gshi),
         # replace ungraded with NA for gshi
         gshi = na_if(gshi, "UG"),
         # replace N with NA for gshi
         gshi = na_if(gshi, "N"),
         # change gshi from character to numeric variable
         gshi = as.numeric(gshi)) |>
        filter(gslo <= 3 & gshi >= 3)

# FILTER AND COLLAPSE DATAFRAME BY LEAID ID

# ______ wake county tps & charter _____
wake <- gis |>
  # filter out to keep only schools that serve 3rd grade
  filter(leaid == "3704720" | 
         leaid == "3700043" |
         leaid == "3700045" |
         leaid == "3700046" |
         leaid == "3700069" |
         leaid == "3700070" |
         leaid == "3700098" |
         leaid == "3700099" |
         leaid == "3700113" |
         leaid == "3700114" |
         leaid == "3700124" |
         leaid == "3700131" |
         leaid == "3700314" |
         leaid == "3700324" |
         leaid == "3700358" |
         leaid == "3700362" |
         leaid == "3700364" |
         leaid == "3700397" |
         leaid == "3700402" |
         leaid == "3700417")

wake$schoolid <- paste0("'", wake$ncessch, "'")
wake$varname <- '"ncessch"'
wake$equation <- paste0(wake$varname, " = ", wake$schoolid, " OR")


write_xlsx(wake, "~/Downloads/wake county g3.xlsx")

# ____ philadelphia tps only ____

phl_leas <- crosswalk |>
  filter(sedalea == 4218990 & year == 2016)

phl <- gis |>
  filter(leaid == "4218990")

phl$schoolid <- paste0("'", phl$ncessch, "'")
phl$varname <- '"ncessch"'
phl$equation <- paste0(phl$varname, " = ", phl$schoolid, " OR")


write_xlsx(phl, "~/Downloads/phl g3.xlsx")

# ____ indianapolis tps only_____
indy <- gis |>
  filter(leaid == "1804770")

indy$schoolid <- paste0("'", indy$ncessch, "'")
indy$varname <- '"ncessch"'
indy$equation <- paste0(indy$varname, " = ", indy$schoolid, " OR")


write_xlsx(indy, "~/Downloads/indy g3.xlsx")

# _____ omaha tps only _______

omaha <- gis |>
  filter(leaid == "3174820")

omaha$schoolid <- paste0("'", omaha$ncessch, "'")
omaha$varname <- '"ncessch"'
omaha$equation <- paste0(omaha$varname, " = ", omaha$schoolid, " OR")


write_xlsx(omaha, "~/Downloads/omaha g3.xlsx")

# _____ boston tps only _____ # not returning any schools? maybe because open enrollment?

bos <- gis |>
  filter(leaid == "2502790")

bos$schoolid <- paste0("'", bos$ncessch, "'")
bos$varname <- '"ncessch"'
bos$equation <- paste0(bos$varname, " = ", bos$schoolid, " OR")


write_xlsx(bos, "~/Downloads/bos g3.xlsx")
