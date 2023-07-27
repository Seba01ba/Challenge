# Challenge

Coding Challenge - Bitcoin Price
Objective:
Develop an automated and scalable process to obtain the average of each 5
days (moving average) of the price of bitcoin in the first quarter of 2022.
Brief:
The finance team needs to analyze the behavior of bitcoin to know if it is feasible
to invest in that currency.
Your task is to obtain this information in an automated way and be prepared for
sudden changes that must be made at the moment.
Tasks:
● Explore Crypto API
○ link: https://www.coingecko.com/en/api/documentation
● Get a list of all coins with id, name and symbol (using Crypto API)
○ Get bitcoin coin id
● Get the price of bitcoin in usd and by date of the first quarter of 2022 (using
Crypto API)
○ Save the information in the database of your choice
● Consume the data previously persisted in the database to make a
window/partition function for every 5 days (spark or pandas)
○ Extra point: Save the information in the database of your choice
● Add your code on github repository
○ Share the link before the interview
● Extra point: using the tool of your choice show the results obtained in a
graph
