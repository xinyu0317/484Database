// find the oldest friend for each user who has a friend. 
// For simplicity, use only year of birth to determine age, if there is a tie, use the one with smallest user_id
// return a javascript object : key is the user_id and the value is the oldest_friend id
// You may find query 2 and query 3 helpful. You can create selections if you want. Do not modify users collection.
//
//You should return something like this:(order does not matter)
//{user1:userx1, user2:userx2, user3:userx3,...}

function oldest_friend(dbname){
	db = db.getSiblingDB(dbname);
	var results = {};

	db.users.aggregate([
		{$project: {_id: 0, user_id: 1, friends: 1}}, 
        {$unwind: "$friends"}, 
        {$out: "flat_users"}
	]);


	db.flat_users.find().forEach(
		function(a){
			db.mutual.insertOne({user_id: a.user_id, friends: a.friends});
			db.mutual.insertOne({user_id: a.friends, friends: a.user_id});
		}
	);

	var dataYOB = {};
	db.users.find().forEach( function(a) {
		dataYOB[a.user_id] = a.YOB;
	});


	db.mutual.aggregate([
        {$group: {_id: "$user_id", friends: {$push: "$friends"}}}, 
        {$out : "mutual2"}
    ]);

	db.mutual2.find().forEach(function(u){
		var old_f = Number.MAX_SAFE_INTEGER;
		var old_birth = Number.MAX_SAFE_INTEGER;
		for (i = 0; i < u.friends.length; i++){
			if (dataYOB[u.friends[i]] < old_birth){
				old_f = u.friends[i];
				old_birth = dataYOB[u.friends[i]];
			}
			if (dataYOB[u.friends[i]] == old_birth){
				if (u.friends[i] < old_f){
					old_f = u.friends[i];
				}
			}
		}
		results[u._id] = old_f;
	});
	return results;
}	