import json 

def lambda_handler(event, context):
    
	cardName = event['cardName']
	condition = event['condition']
	number = event['number']

	price = getCarPrice(cardName, condition, number)
	print("Base on given condition price is :",price)

	return {
		'cardName':cardName,
		'condition': condition,
		'number': number, 
		'PRICE':price
	}

def getCarPrice(cardName, condition, number):
	print("Name is:",cardName, "*", "the condition=",condition, "& number is=", number)

	if (cardName == "Najmeh" and condition == "passed" and int(number) <= 50):
		return "given number is less than 50"
	elif (cardName == "Najmeh" and condition == "passed" and int(number) > 50):
		return "given number is greter than 50"
	else:
		return "Above standard"
