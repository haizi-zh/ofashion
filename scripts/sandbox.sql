# 临时表
DROP TABLE tmp;
DROP TABLE tmp1;

SELECT COUNT(*) FROM tmp WHERE date > '2013-11-11';
CREATE TEMPORARY TABLE tmp1 (SELECT COUNT(*) AS cnt,idproducts FROM tmp GROUP BY idproducts);
SELECT * FROM tmp1 order by cnt desc;
SELECT * FROM tmp;

# 彻底清除某个品牌
# DELETE FROM products WHERE brand_id=10308;
# DELETE FROM original_tags WHERE brand_id=10308;

# 彻底清除某个品牌的图像数据
# DELETE FROM p1 USING images_store AS p1, products_image AS p2 WHERE p1.checksum=p2.checksum AND p2.brand_id=10308;

# CREATE TEMPORARY TABLE tmp

# 各国家的单品数量
SELECT COUNT(*),region FROM products WHERE brand_id=10308 GROUP BY region;
# 最新更新的单品
SELECT * FROM products WHERE brand_id=10066 ORDER BY touch_time DESC LIMIT 100;
# 某款单品的原始标签
SELECT * FROM products AS p1 JOIN products_original_tags AS p2 ON p1.idproducts=p2.idproducts JOIN original_tags AS p3 ON p2.id_original_tags=p3.idmappings
WHERE p1.model='w4006';
# 单品的标签映射表（仅限cn）
SELECT  * FROM original_tags WHERE brand_id=10029 ORDER BY tag_type,tag_name;

select * from original_tags where brand_id=10057 and mapping_list = '["其他"]';

# 各单品的价格记录
CREATE TEMPORARY TABLE tmp (SELECT p2.date,p2.price,p2.currency,p1.idproducts,p1.brand_id,p1.region,p1.name FROM products AS p1 JOIN products_price_history AS p2 ON p1.idproducts=p2.idproducts 
WHERE brand_id=10057);
# 删除价格记录
DELETE FROM p2 USING products AS p1, products_price_history AS p2 WHERE p1.idproducts=p2.idproducts AND p1.brand_id=10093;
delete from products_price_history;

# 各单品的MFashion标签
SELECT p3.tag,p1.*,p2.* FROM products AS p1 JOIN products_mfashion_tags AS p2 ON p1.idproducts=p2.idproducts JOIN mfashion_tags AS p3 ON p2.id_mfashion_tags=p3.idmfashion_tags
WHERE p1.model=44527711;# p1.brand_id=10093 ORDER BY p1.idproducts;
# 各单品对应的图片
SELECT p3.*,p1.* FROM products AS p1 LEFT JOIN products_image AS p2 ON p1.model=p2.model AND p1.brand_id=p2.brand_id LEFT JOIN images_store AS p3 ON p2.checksum=p3.checksum
WHERE p1.brand_id=10308;
# 没有图片的单品
SELECT p2.*,p1.* FROM products AS p1 LEFT JOIN products_image AS p2 ON p1.model=p2.model AND p1.brand_id=p2.brand_id
WHERE checksum IS NULL AND p1.region='cn' AND p1.brand_id=10226;
# 查看release
SELECT * FROM products_release WHERE brand_id=10135;