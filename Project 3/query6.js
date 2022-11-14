

function find_average_friendcount(dbname){
    db = db.getSiblingDB(dbname);
    var dic={};
    var cursor=db.users.aggregate([
        {$unwind:"$friends"},
        {$lookup:{from:"users",localField:"friends",foreignField:"user_id",as:"friendsItem"}},
        {$unwind:"$friendsItem"},
        {$project:
            {
                user_id:1,
                friend:"$friendsItem.user_id",
                YOB:1,
                friendYOB:"$friendsItem.YOB",
                _id:0,
                sub:{$subtract:["$user_id","$friends"]},
                subyob:{$subtract:["$friendsItem.YOB","$YOB"]}
            }
        }
    ]);
    var i=0;
    cursor.forEach(function(item){
        i=i+1;
    })
    var result = 1.0 * i / db.users.find().count();
    return result;
  }

  