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