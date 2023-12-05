SELECT
  b.bookId,
  lb.Title,
  COUNT(*) NumRecs
FROM
  `DetectedBooks` b,
  libbook lb,
  librecs lr
WHERE
  lb.BookId = b.bookId
  and lr.bid = b.bookId
GROUP BY
  b.bookId,
  lb.Title
order by
  NumRecs DESC;

CREATE TABLE BooksFT AS
SELECT
  lb.BookId,
  lb.Title,
  lan.FirstName,
  lan.MiddleName,
  lan.LastName,
  lan.NickName,
  lsn.SeqName
FROM
  `libbook` lb,
  libavtor la,
  libavtorname lan,
  libseqname lsn,
  libseq ls
WHERE
  lb.BookId = la.BookId
  AND la.AvtorId = lan.AvtorId
  and ls.BookId = lb.BookId
  and lsn.SeqId = ls.SeqId;

SELECT
  *,
  MATCH(Title, SeqName) AGAINST (
    'Эра Огня 5. Мятежное пламя' in natural language mode
  ) As relevance_title,
  match(FirstName, LastName, MiddleName, NickName) AGAINST('Василий Криптонов' in natural language mode) as relevance_author
FROM
  `BooksFT`
WHERE
  MATCH(Title, SeqName) AGAINST (
    'Эра Огня 5. Мятежное пламя' in natural language mode
  )
  and match(FirstName, LastName, MiddleName, NickName) AGAINST('Василий Криптонов' in natural language mode);




DROP VIEW IF EXISTS TopRatedDetectedBooks;

CREATE VIEW `TopRatedDetectedBooks` AS
SELECT
  `b`.`BookId` AS `bookId`,
  `lb`.`Title` AS `Title`,
  REPLACE(b.Annotation, '\n', '') AS Annotation, -- Replace new lines in Annotation
  b.Authors,
  count(0) AS `NumRecs`
FROM
  (
    (
      `DetectedBooks` `b`
      join `libbook` `lb`
    )
    join `librecs` `lr`
  )
WHERE
  `lb`.`BookId` = `b`.`BookId`
  AND `lr`.`bid` = `b`.`BookId`
GROUP BY
  `b`.`BookId`,
  `lb`.`Title`
ORDER BY
  count(0) DESC;
