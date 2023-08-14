library(gt)  
library(gtsummary)
library(tidyverse)
library(lme4)
library(caret)
library(corrr)
library(knitr) 


base.mod %>%
  tbl_regression(exponentiate = TRUE, intercept=TRUE) %>%
  as_gt() %>%
  gtsave('basic.mod.tex', path='./GitHub/dissertation/pdev')

mod.ex.ante %>%
  tbl_regression(exponentiate = TRUE, intercept=TRUE) %>%
  as_gt() %>%
  gtsave('ex.ante.tex', path='./GitHub/dissertation/pdev/')

mod.step.final %>%
  tbl_regression(exponentiate = TRUE, intercept=TRUE) %>%
  as_gt() %>%
  gtsave('mod.step.final.tex', path='./GitHub/dissertation/pdev/')