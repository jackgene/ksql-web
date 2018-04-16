// Custom mode for KSQL, based on SQL.
(function(mod) {
  if (typeof exports == "object" && typeof module == "object") // CommonJS
    mod(require("../../../../lib/codemirror"));
  else if (typeof define == "function" && define.amd) // AMD
    define(["../../../../lib/codemirror"], mod);
  else // Plain browser env
    mod(CodeMirror);
})(function(CodeMirror) {
"use strict";

(function() {
  "use strict";

  function set(str) {
    var obj = {}, words = str.split(" ");
    for (var i = 0; i < words.length; ++i) obj[words[i]] = true;
    return obj;
  }

  CodeMirror.defineMIME("text/x-ksql", {
    name: "sql",
    keywords: set("abs alter and arraycontains as asc between by cast ceil concat count create delete desc describe distinct drop explain extended extractjsonfield floor from group having hopping in insert into is join lcase left len like limit list max min not on or order properties queries random round second select session set show size stream streams stringtotimestamp substring sum table tables timestamptostring topics topk topkdistinct trim tumbling ucase union update values where window with"),
    builtin: set("bool boolean bit blob enum long longblob longtext medium mediumblob mediumint mediumtext time timestamp tinyblob tinyint tinytext text bigint int int1 int2 int3 int4 int8 integer float float4 float8 double char varbinary varchar varcharacter precision real date datetime year unsigned signed decimal numeric"),
    atoms: set("false true null unknown"),
    operatorChars: /^[*+\-%<>!=]/,
    dateSQL: set("date time timestamp"),
    support: set("ODBCdotTable doubleQuote binaryNumber hexNumber")
  });
}());

});