CREATE KEYSPACE IF NOT EXISTS local_hierarchy_store with replication = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE IF NOT EXISTS local_hierarchy_store.questionset_hierarchy (
  identifier text,
  hierarchy text,
  instructions text,
  PRIMARY KEY (identifier)
);

CREATE KEYSPACE IF NOT EXISTS local_question_store with replication = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE IF NOT EXISTS local_question_store.question_data (
  identifier text,
  body blob,
  editorState text,
  answer blob,
  solutions text,
  instructions text,
  hints text,
  media text,
  responseDeclaration text,
  interactions text,
  PRIMARY KEY (identifier)
);


INSERT INTO local_question_store.question_data(identifier, body, editorstate, answer, solutions, media, responsedeclaration) VALUES (
'do_113188615625731',
textAsBlob('<div class="question-body"><div class="mcq-title"><p>Color of Sky is?</p></div><div data-choice-interaction="response1" class="mcq-vertical"></div></div>'),
'{"question":"<p>Capital of india is?</p>","answer":"<p>New Delhi</p>"}',
textAsBlob('1'),
'[{"id": "c012a8a9-f78b-6ddb-3ac2-bd1f38c7850b", "type": "html", "value": "<p>Color of sky is blue&nbsp;</p><figure class=\"image\"><img src=\"https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11310507846892748812026/artifact/icon.png\" alt=\"do_11310507846892748812026\" data-asset-variable=\"do_11310507846892748812026\"></figure>"}]',
'[{"id":"do_1127129497561497601326","type":"image","src":"/content/do_1127129497561497601326.img/artifact/sunbird_1551961194254.jpeg","baseUrl":"https://sunbirddev.blob.core.windows.net/sunbird-content-dev"}]',
'{"response1": {"maxScore": 1, "cardinality": "single", "type": "integer", "correctResponse": { "value": "1", "outcomes": {"SCORE": 1}}}}'
);


INSERT INTO local_question_store.question_data(identifier, body, editorstate, answer, solutions, media, responsedeclaration, interactions) VALUES (
'do_113188615625731.img',
textAsBlob('<div class="question-body"><div class="mcq-title"><p>Color of Sky is?</p></div><div data-choice-interaction="response1" class="mcq-vertical"></div></div>'),
'{"question":"<p>Capital of india is?</p>","answer":"<p>New Delhi</p>"}',
textAsBlob('1'),
'[{"id": "c012a8a9-f78b-6ddb-3ac2-bd1f38c7850b", "type": "html", "value": "<p>Color of sky is blue&nbsp;</p><figure class=\"image\"><img src=\"https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11310507846892748812026/artifact/icon.png\" alt=\"do_11310507846892748812026\" data-asset-variable=\"do_11310507846892748812026\"></figure>"}]',
'[{"id": "do_11318931140144332811620", "type": "image", "src": "https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11318931140144332811620/artifact/i.png", baseUrl": "https://dock.sunbirded.org"}, {"id": "do_11310507846892748812026", "type": "image", "src": "https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11310507846892748812026/artifact/icon.png", "baseUrl": "https://dock.sunbirded.org"}]',
'{"response1": {"maxScore": 1, "cardinality": "single", "type": "integer", "correctResponse": { "value": "1", "outcomes": {"SCORE": 1}}}}',
'{"response1": {"type": "choice","options": [{"label": "<p>Red&nbsp;</p><figure class=\"image\"><img src=\"https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11318931140144332811620/artifact/i.png\" alt=\"do_11318931140144332811620\" data-asset-variable=\"do_11318931140144332811620\"></figure>","value": 0}]}}'
);


INSERT INTO local_hierarchy_store.questionset_hierarchy(identifier, hierarchy) VALUES ('do_123', '{ "name": "QS1", "identifier": "do_123", "children": [{"identifier": "do_124", "objectType": "QuestionSet", "visibility": "Parent"}]}');
INSERT INTO local_hierarchy_store.questionset_hierarchy(identifier, hierarchy) VALUES ('do_321', '{ "name": "QS1", "identifier": "do_123", "children": [{"identifier": "do_124", "objectType": "QuestionSet", "visibility": "Parent"}]}');
INSERT INTO local_hierarchy_store.questionset_hierarchy(identifier, hierarchy) VALUES ('do_123.img', '{ "name": "QS1", "identifier": "do_123", "children": [{"identifier": "do_124", "objectType": "QuestionSet", "visibility": "Parent"}, {"identifier": "do_125", "objectType": "QuestionSet", "visibility": "Parent"}]}');
INSERT INTO local_hierarchy_store.questionset_hierarchy(identifier, hierarchy) VALUES ('do_21385612453673369613688', '{"identifier":"do_21385612453673369613688","children":[{"parent":"do_21385612453673369613688","instructions":{"default":"sample instructions"},"code":"section-1","allowSkip":"Yes","containsUserData":"No","description":"Section-1","language":["English"],"mimeType":"application/vnd.sunbird.questionset","showHints":"No","createdOn":"2023-08-07T15:17:48.218+0000","objectType":"QuestionSet","scoreCutoffType":"AssessmentLevel","primaryCategory":"Practice Question Set","children":[{"parent":"do_21385612466764185613693","code":"q1","description":"Q1","language":["English"],"mimeType":"application/vnd.sunbird.question","createdOn":"2023-08-07T15:17:48.208+0000","objectType":"Question","primaryCategory":"Multiple Choice Question","contentDisposition":"inline","lastUpdatedOn":"2023-08-07T15:17:48.208+0000","contentEncoding":"gzip","showSolutions":"No","allowAnonymousAccess":"Yes","identifier":"do_21385612466755993613689","lastStatusChangedOn":"2023-08-07T15:17:48.208+0000","visibility":"Parent","showTimer":"No","index":1,"qType":"MCQ","maxScore":1,"languageCode":["en"],"bloomsLevel":"create","version":1,"versionKey":"1691421468218","showFeedback":"No","license":"CC BY 4.0","interactionTypes":["choice"],"depth":2,"compatibilityLevel":4,"name":"Q1","status":"Draft"},{"parent":"do_21385612466764185613693","code":"q2","description":"Q2","language":["English"],"mimeType":"application/vnd.sunbird.question","createdOn":"2023-08-07T15:17:48.217+0000","objectType":"Question","primaryCategory":"Subjective Question","contentDisposition":"inline","lastUpdatedOn":"2023-08-07T15:17:48.217+0000","contentEncoding":"gzip","showSolutions":"No","allowAnonymousAccess":"Yes","identifier":"do_21385612466763366413691","lastStatusChangedOn":"2023-08-07T15:17:48.217+0000","visibility":"Parent","showTimer":"Yes","index":2,"qType":"SA","maxScore":1,"languageCode":["en"],"version":1,"versionKey":"1691421468219","showFeedback":"No","license":"CC BY 4.0","depth":2,"compatibilityLevel":4,"name":"Q2","status":"Draft"}],"contentDisposition":"inline","lastUpdatedOn":"2023-08-07T15:17:48.218+0000","contentEncoding":"gzip","generateDIALCodes":"No","showSolutions":"No","trackable":{"enabled":"No","autoBatch":"No"},"allowAnonymousAccess":"Yes","identifier":"do_21385612466764185613693","lastStatusChangedOn":"2023-08-07T15:17:48.218+0000","requiresSubmit":"No","visibility":"Parent","showTimer":"Yes","index":1,"setType":"materialised","languageCode":["en"],"version":1,"versionKey":"1691421468218","showFeedback":"No","license":"CC BY 4.0","depth":1,"compatibilityLevel":5,"name":"Section-1","navigationMode":"non-linear","allowBranching":"No","shuffle":true,"status":"Draft"}]}');