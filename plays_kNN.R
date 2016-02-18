#k-NN model for pass/play prediction

library(class)
library(gmodels)

data <- read.csv(file.choose()) #using plays.csv

ind <- sample(2, nrow(data), replace=TRUE, prob=c(0.8, 0.2))
data.training <- data[ind==1, 1:9]
data.test <- data[ind==2, 1:9]
data.trainLabels <- data[ind==1, 10]
data.testLabels <- data[ind==2, 10]
data_pred <- knn(train = data.training, test = data.test, cl = data.trainLabels, k=5)
CrossTable(x = data.testLabels, y = data_pred, prop.chisq=FALSE)

accuracy = sum(data_pred == data.testLabels)/length(data_pred)

# Overall Accuracy: 58.6%
