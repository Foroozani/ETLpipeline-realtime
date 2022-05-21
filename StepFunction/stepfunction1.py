import json 

def lambda_handler(event, context):
	input_param = {
		"cardName": "Najmeh",
		"condition": "passed",
		"number": "90"
	}
	print('Response from first Lambda', input_param)
	return input_param
