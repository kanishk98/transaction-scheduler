let arr = [];
let items = [];
const fs = require('fs');

function prep_program(numberOfTransactions, numberOfDataItems) {
	for(let i = 0; i < numberOfDataItems; ++i) {
		items.push(String(i));
	}
	for(let i = 0; i < numberOfTransactions; ++i) {
		let item = Math.floor(Math.random() * numberOfDataItems);
		let op = {};
		op.item = {};
		op.item.variable = item;
		op.item.kind = Math.random() > 0.5? true : false;
		op.tid = String(i);
		arr.push(op);
	}
}

prep_program(15, 2);
fs.writeFile('./random_req.json', JSON.stringify(arr), function(err, data) {
	if (err) {
		console.log(err);
	}
});