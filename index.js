var elasticsearch = require('elasticsearch');
var mysql = require('mysql');

var client = new elasticsearch.Client({
    host: 'http://localhost:9200',
    log: 'trace', // <- verbose method (DEVELOPMENT ONLY!)
});

var createIndex = function(name) {
    client.indices.create({
        index: name
    }, function(err, resp, status) {
        if (err) {
            console.log(err);
        } else {
            console.log("create", resp);
        }
    });
}

var dataKeywordFormat = function(res) {
    return data = {
        "index": "keyword",
        "type": "_doc",
        "id": res.input_id,
        "body": {
            "keyword": res.keyword,
            "suggestions": res.suggestions
        }
    }
}

var dataTagFormat = function(res) {
    return data = {
        "index": "tag",
        "type": "_doc",
        "id": res.id,
        "body": {
            "tags": res.tags,
            "slug": res.slug,
            "keyword": res.keyword,
            "latitude_1": res.latitude_1,
            "longitude_1": res.longitude_1,
            "latitude_2": res.latitude_2,
            "longitude_2": res.longitude_2,
            "type": res.type,
            "area_city": res.area_city,
            "area_subdistrict": res.area_subdistrict,
            "price_min": res.price_min,
            "price_max": res.price_max,
            "gender": res.gender,
            "rent_type": res.rent_type
        }
    }
}

var dataRoomFormat = function(res) {
    // Manipulate 'completion' fields value
    var name = (res.name == null) ? '' : res.name;
    var address = (res.address == null) ? '' : res.address;
    var area_city = (res.area_city == null) ? '' : res.area_city;
    var area_subdistrict = (res.area_subdistrict == null) ? '' : res.area_subdistrict;
    var area_big = (res.area_big == null) ? '' : res.area_big;

    return data = {
        "index": "room",
        "type": "_doc",
        "id": res.id,
        "body": {
            "code": res.code,
            "song_id": res.song_id,
            "name": name,
            "address": address,
            "area_subdistrict": area_subdistrict,
            "area_city": area_city,
            "area_big": area_big,
            "location_id": res.location_id,
            "latitude": res.latitude,
            "longitude": res.longitude,
            "gender": res.gender,
            "status": res.status,
            "room_available": res.room_available,
            "room_count": res.room_count,
            "is_promoted": (res.is_promoted) ? true : false,
            "slug": res.slug,
            "is_booking": (res.is_booking == 1) ? true : false,
            "price_daily": (res.price_daily != null) ? res.price_daily : 0,
            "price_weekly": (res.price_weekly != null) ? res.price_weekly : 0,
            "price_monthly": (res.price_monthly != null) ? res.price_monthly : 0,
            "price_yearly": (res.price_yearly != null) ? res.price_yearly : 0,
            "price_remark": res.price_remark,
            "fac_room_other": res.fac_bath_other,
            "fac_bath_other": res.fac_bath_other,
            "fac_share_other": res.fac_share_other,
            "fac_near_other": res.fac_near_other,
            "description": res.description,
            "size": res.size,
            "is_verified_address": (res.is_verified_address == 1) ? true : false,
            "is_verified_phone": (res.is_verified_phone == 1) ? true : false,
            "is_verified_kost": (res.is_verified_kost == 1) ? true : false,
            "is_visited_kost": (res.is_verified_kost == 1) ? true : false,
            "floor": res.floor,
            "furnished": (res.furnished == 1) ? true : false,
            "view_count": res.view_count,
            "unit_type": res.unit_type,
            "photo_count": res.photo_count,
            "unit_number": res.unit_number,
            "youtube_id": res.youtube_id,
            "promotion": (res.promotion) ? true : false,
            "kost_updated_date": res.kost_updated_date,
            "created_at": res.created_at,
            "updated_at": res.updated_at
        }
    }
}

var startIndexing = function(index_name) {
    require('dotenv').config();
    var pool = mysql.createPool({
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        password: process.env.DB_PASS,
        database: process.env.DB_DATABASE
    });

    if (index_name == 'room') {
        var table = process.env.ROOM_TABLE + ' a';
        var select = "SELECT * FROM ";
        var where = ' WHERE a.deleted_at IS NULL';
    } else if (index_name == 'tag') {
        var table = process.env.LANDING_TABLE + ' a';
        var select = "SELECT * FROM ";
        var where = ' LEFT JOIN landing b ON b.id = a.landing_id LEFT JOIN tagging c ON c.id = a.tagging_id WHERE a.deleted_at IS NULL';
    } else { // <- keyword
        var table = process.env.KEYWORD_TABLE + ' a';
        var select = "SELECT a.input_id, b.keyword, a.suggestion, a.area, a.latitude, a.longitude FROM ";
        var where = ' LEFT JOIN input_keyword b on b.id = a.input_id WHERE b.deleted_at IS NULL';
    }

    var countQuery = "SELECT count(*) as total FROM " + table + where;
    var chunkSize = process.env.CHUNK_SIZE;

    pool.getConnection(function(err, connection) {
        if (err) {
            connection.release();
            console.log("Error on getConnection:", err);
            return;
        }

        connection.query(countQuery, {}, function(err, result) {
            if (err) {
                connection.release();
                console.log("Error on getConnection:", err);
                return;
            }

            if (result && result[0]) {
                var totalRows = result[0]['total'];
                console.log("Total rows in DB:", totalRows);
                var periods = Math.ceil(totalRows / chunkSize)
                console.log("Total chunks:", periods);

                var selectQuery = select + table + where + " ORDER BY a.id DESC LIMIT ";
                var counter = 1;

                for (var i = 0; i < periods; i++) {
                    var offset = i * chunkSize;
                    var runQuery = selectQuery + offset + "," + chunkSize;

                    connection.query(runQuery, {}, function(err, results) {
                        if (err) {
                            console.log("Error on runQuery:", err);
                            return;
                        }

                        if (index_name == 'room') {
                            var stopped = false;
                            for (var j = 0; j < results.lengthngth; j++) {
                                if (!stopped) {
                                    client.index(dataRoomFormat(results[j]), function(err, resp, status) {
                                        if (err) {
                                            console.log('Indexing error:', status, err);
                                            stopped = true;
                                        } else {
                                            console.log('Indexing status: OK');
                                        }
                                    });
                                }
                            }

                        } else if (index_name == 'tag') {
                            var tempData = []; // <- for landing_id grouping
                            for (var j = 0; j < results.length; j++) {
                                var $id = results[j].landing_id;

                                // group by 'landing_id'
                                if (tempData[$id] == undefined) {
                                    tempData[$id] = results[j];
                                }

                                // compile taggings
                                if (tempData[$id].tags == undefined) {
                                    tempData[$id].tags = [];
                                }

                                tempData[$id].tags.push(results[j].name);
                            }

                            // start indexing
                            var stopped = false;
                            tempData.forEach(function(dt) {
                                if (!stopped) {
                                    client.index(dataTagFormat(dt), function(err, resp, status) {
                                        if (err) {
                                            console.log('Indexing error:', status, err);
                                            stopped = true;
                                        } else {
                                            console.log('Indexing status: OK');
                                        }
                                    });
                                }
                            });

                        } else { // << index_name == 'keyword'
                            var tempData = []; // <- for input_id grouping
                            for (var j = 0; j < results.length; j++) {
                                var $id = results[j].input_id;

                                // group by 'input_id'
                                if (tempData[$id] == undefined) {
                                    tempData[$id] = results[j];
                                }

                                // compile suggestions
                                if (tempData[$id].suggestions == undefined) {
                                    tempData[$id].suggestions = [];
                                }

                                tempData[$id].suggestions.push({
                                    'suggestion': results[j].suggestion,
                                    'area': results[j].area,
                                    'latitude': results[j].latitude,
                                    'longitude': results[j].longitude
                                });
                            }

                            // start indexing
                            var stopped = false;
                            tempData.forEach(function(dt) {
                                // console.log(dataKeywordFormat(dt));
                                if (!stopped) {
                                    client.index(dataKeywordFormat(dt), function(err, resp, status) {
                                        if (err) {
                                            console.log('Indexing error:', status, err);
                                            stopped = true;
                                        } else {
                                            console.log('Indexing status: OK');
                                        }
                                    });
                                }
                            });
                        }
                    });

                    counter++;
                }

                connection.release();
            }

        });
    });

    // return indexedData;
}

// Main executions
console.log("Type 'keyword', 'room' or 'tag' to start indexing.");
var stdin = process.openStdin();
stdin.addListener("data", function(d) {
    var type = d.toString().trim();
    if (type == 'keyword' || type == 'room' || type == 'tag') {
        console.log("Begin indexing: " + type + "...");
        // createIndex('test');
        startIndexing(type);
    } else {
        console.log("Type 'keyword', 'room' or 'tag' to start indexing.");
    }
});
