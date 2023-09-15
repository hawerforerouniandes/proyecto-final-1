CREATE TABLE public.questionnaire (
    id serial PRIMARY KEY,
    id_user varchar(100),
    name varchar(100),
    type_questionnaire varchar(100),
    id_question varchar,
    respuesta varchar(1),
    timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
    create_timestamp timestamp DEFAULT CURRENT_TIMESTAMP
);
