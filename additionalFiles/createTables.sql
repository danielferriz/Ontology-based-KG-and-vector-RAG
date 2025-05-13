DROP TABLE IF EXISTS Examples CASCADE;
DROP TABLE IF EXISTS Prompts CASCADE;
DROP TYPE IF EXISTS queryTypes CASCADE;

CREATE TYPE queryTypes AS ENUM ('user','system','assistant');

CREATE TABLE Prompts (
	prompt_id 			BIGSERIAL PRIMARY KEY,
	general_prompt_id 	BIGINT NOT NULL,
	sequence_id 		INT NOT NULL,
	lang 				VARCHAR(10) NOT NULL,
	type 				queryTypes NOT NULL,
	description 		TEXT NOT NULL,
	prompt 				TEXT NOT NULL,
	variables 			VARCHAR(100)
);

CREATE INDEX Prompts_idx ON Prompts (general_prompt_id, sequence_id, lang);

CREATE TABLE Examples (
	example_id 			BIGSERIAL PRIMARY KEY,
	general_example_id 	BIGINT NOT NULL,
	sequence_id 		INT NOT NULL,
	lang 				VARCHAR(10) NOT NULL,
	prompt_id 			BIGINT NOT NULL REFERENCES Prompts(prompt_id),
	example 			TEXT NOT NULL,
	variables 			VARCHAR(100)
);

CREATE INDEX Examples_idx ON Examples (general_example_id, sequence_id, lang);