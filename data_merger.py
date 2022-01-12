from  validation_classes import FilmWork

class Data_Merger:
    def __init__(self):
        self. desired_structure = {
            "fw_id": "",
            "imdb_rating": "",
            "genre": [],
            "title": "",
            "description": "",
            "director": "",
            "actors_names": [],
            "writers_names": [],
            "writers": [],
            "actors": [],
        }
        self.result_merged = self.desired_structure
    def  combine_tables(self,obj:dict):
        self.result_merged ["fw_id"] = obj['film_id']
        self.result_merged ['imdb_rating'] = obj['rating']
        self.result_merged ['title'] = obj['title']
        self.result_merged ["description"] = obj['description']
        if obj["role"] == "director" and obj["full_name"] != self.result_merged ["director"]:
            self.result_merged ['director'] = obj['full_name']
        if obj["role"] == "actor" and obj["full_name"] not in self.result_merged ["actors_names"]:
            self.result_merged ["actors_names"].append(obj["full_name"]),
            self.result_merged ["actors"].append({"id": obj['person_id'],
                                                "name": obj["full_name"]})
        if obj["role"] == "writer" and obj["full_name"] not in self.result_merged ["writers_names"]:
            self.result_merged ["writers_names"].append(obj["full_name"]),
            self.result_merged ["writers"].append({"id": obj['person_id'],
                                                 "name": obj["full_name"]})

    def validate_and_return(self):
        validated_result = FilmWork.parse_obj(self.result_merged)
        return  validated_result
    def clean(self):
        self.result_merged = {
            "fw_id": "",
            "imdb_rating": "",
            "genre": [],
            "title": "",
            "description": "",
            "director": "",
            "actors_names": [],
            "writers_names": [],
            "writers": [],
            "actors": [],
        }