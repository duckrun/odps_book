#set params
inputTable = 'tmall_train_sample'
features = ['brand_id', 'buy_cnt', 'click_d7', 'collect_d7', 'shopping_cart_d7', 'click_d3', 'collect_d3', 'shopping_cart_d3', 'cvr']
isFeatureContinuous = [False, True, True, True, True, True, True, True, True]
label = 'flag'
prefix = "tt_"
modelTable = prefix + inputTable + "_rf_model"
validateTable = "tmall_test_sample"

#train
rfModel = Classification.RandomForest.train(inputTable, features, isFeatureContinuous,
             label, modelTable, 10)
#predict
predictOutputTable = prefix + validateTable + "_predict"
Classification.RandomForest.predict(validateTable, rfModel, predictOutputTable,
		   isBin = True, labelValueToPredict = '1')

#validate
evalOutputTable = prefix + validateTable + "_eval"
cm=Classification.Evaluation.calcConfusionMatrix(validateTable, label, predictOutputTable,
    "conclusion", evalOutputTable)

#show result
show(cm)
