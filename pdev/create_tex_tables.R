library(gt)  
library(gtsummary)
library(tidyverse)
library(rsq)
library(lme4)
library(caret)
library(corrr)
library(knitr) 

df4 <- read.csv('../../../sadamerdji/Desktop/dissertation/pdev/cleaned_rhna4_data.csv')

df4 <- select(df4, -MapBlkLot_Master)

df4$Developed <- df4$Developed > 0
df4 <- df4[df4$ROOMS >= 0,]
df4 <- df4[df4$FBA >= 0,]
df4 <- df4[df4$YRBLT <= 2023,]

# There are 19 properties supposedly built before SF's founding. I set to year 0 
df4[0 < df4$YRBLT & df4$YRBLT <= 1776,]$YRBLT <- 0

df4['yearBuiltMissing'] <- df4$YRBLT == 0

# Area code that affects taxes
df4$RP1TRACDE <- as.factor(df4$RP1TRACDE)

# Property records are split up into volumes for organizational purposes.
# This is likely highly correlated with neighborhoods or census tracts.
df4$RP1VOLUME <- as.factor(df4$RP1VOLUME)

df4$inInventory <- ifelse(df4$inInventory == 'True', 1, 0)#

base.mod <- glm(Developed ~ inInventory, df4, family='binomial')
summary(base.mod)

base.mod.latex <- base.mod %>%
  tbl_regression(exponentiate = TRUE, intercept=TRUE) %>%
  as_gt() %>%
  gtsave('basic.mod.tex', path='../../../sadamerdji/Desktop/dissertation/pdev/')

mod.log %>%
  tbl_regression(exponentiate = TRUE, intercept=TRUE) %>%
  as_gt() %>%
  gtsave('step.mod.tex', path='../../../sadamerdji/Desktop/dissertation/pdev/')
