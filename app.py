from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def index():
    data = {
        "1900": [
            {"name": "eggs", "label": "Egg 🥚", "price": "10c"},
            {"name": "milk", "label": "Milk Gal 🥛", "price": "30c"}
        ],
        "1910": [
            {"name": "bread", "label": "Bread 🍞", "price": "15c"},
            {"name": "butter", "label": "Butter 🧈", "price": "25c"}
        ],
        "1920": [
            {"name": "sugar", "label": "Sugar 🍬", "price": "20c"},
            {"name": "tea", "label": "Tea ☕", "price": "40c"}
        ],
        "1930": [
            {"name": "coffee", "label": "Coffee ☕", "price": "35c"},
            {"name": "rice", "label": "Rice 🍚", "price": "50c"}
        ],
        "1940": [
            {"name": "meat", "label": "Meat 🍖", "price": "$0.50"},
            {"name": "cheese", "label": "Cheese 🧀", "price": "$0.60"}
        ],
        "1950": [
            {"name": "butter", "label": "Butter 🧈", "price": "$0.70"},
            {"name": "soda", "label": "Soda 🥤", "price": "$0.30"}
        ],
        "1960": [
            {"name": "cereal", "label": "Cereal 🥣", "price": "$1.00"},
            {"name": "juice", "label": "Juice 🍹", "price": "$0.80"}
        ],
        "1970": [
            {"name": "chips", "label": "Chips 🍟", "price": "$0.90"},
            {"name": "pop", "label": "Pop 🥤", "price": "$0.50"}
        ],
        "1980": [
            {"name": "chocolate", "label": "Chocolate 🍫", "price": "$1.20"},
            {"name": "soda", "label": "Soda 🥤", "price": "$1.00"}
        ],
        "1990": [
            {"name": "burger", "label": "Burger 🍔", "price": "$2.00"},
            {"name": "fries", "label": "Fries 🍟", "price": "$1.50"}
        ],
        "2000": [
            {"name": "pizza", "label": "Pizza 🍕", "price": "$3.00"},
            {"name": "soda", "label": "Soda 🥤", "price": "$1.00"}
        ],
        "2010": [
            {"name": "sushi", "label": "Sushi 🍣", "price": "$5.00"},
            {"name": "ramen", "label": "Ramen 🍜", "price": "$4.00"}
        ],
        "2020": [
            {"name": "avocado toast", "label": "Avocado Toast 🥑", "price": "$6.00"},
            {"name": "matcha latte", "label": "Matcha Latte 🍵", "price": "$4.50"}
        ]
    }
    return render_template('base.html', data=data)

if __name__ == '__main__':
    app.run(debug=True)
