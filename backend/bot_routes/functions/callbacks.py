from bot_routes.classes.CallbackData import CallbackData

bills_callback = CallbackData("action", "bill_id")
change_payment_date_bill_callback = CallbackData("bill_id", "data")
create_select_account_payment_callback = CallbackData("bill_id", "account_id")
