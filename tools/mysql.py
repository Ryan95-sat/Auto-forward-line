# -*- coding: utf-8 -*-
import pymysql
import traceback
from dbutils.pooled_db import PooledDB
from loguru import logger

from tools.settings import CRAWLER_MYSQL_CONF


class MysqlPool:
    """
    mysql 连接池
    """
    __pool = None

    def __init__(self, db=None, mysql_conf=CRAWLER_MYSQL_CONF):
        """
        :param db: 库名
        :param mysql_conf: 初始化的数据库信息
        """
        if not db:
            db = mysql_conf['db']
        self.mysql_conf = mysql_conf
        self._conn = self.get_conn(mysql_conf=mysql_conf)
        self._cursor = self._conn.cursor()

    def get_conn(self, mysql_conf=None):
        """
        默认是爬虫数据库
        这个方法除了在初始化时调用，还可以用来单独返回一个 pymysql.Connection 对象。
        为了方便在外部调用，方法名由于原来的双下划线（私有属性）改为没有下划线（公有属性）
        :param db: 库名
        :param mysql_conf: 爬虫数据库信息
        # mincached : 启动时开启的空连接数量(0代表开始时不创建连接)
        # maxcached : 连接池最大可共享连接数量(0代表不闲置连接池大小)
        # maxshared : 共享连接数允许的最大数量(0代表所有连接都是专用的)如果达到了最大数量,被请求为共享的连接将会被共享使用
        # maxconnections : 创建连接池的最大数量(0代表不限制)
        # blocking : 达到最大数量时是否阻塞(0或False代表返回一个错误<toMany......>; 其他代表阻塞直到连接数减少,连接被分配)
        # maxusage : 单个连接的最大允许复用次数(0或False代表不限制的复用).当达到最大数时,连接会自动重新连接(关闭和重新打开)
        # setsession : 用于传递到数据库的准备会话，如 [”set name UTF-8″]
        """
        conn_args = {  # 默认连接池参数，有定制化配置池需求可以根据上面的参数修改该类实例化时传入的mysql_conf
            'mincached': 1,
            'maxcached': 20,  # 由于目前很多脚本都是单独实例化连接，所以这里不需要设置很大。如果大量脚本共用一个实例则需要改大
            'setsession': ['SET AUTOCOMMIT = 1'],
            'cursorclass': pymysql.cursors.DictCursor,
            'charset': 'utf8mb4',
            'creator': pymysql,
        }
        if not mysql_conf:
            mysql_conf = self.mysql_conf
        conn_args.update(mysql_conf)
        if self.__pool is None:
            self.__pool = PooledDB(**conn_args)
        return self.__pool.connection()

    def get_all(self, sql, param=None):
        # param查询条件值(元组\列表)
        # 返回list
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            count = self.__query(sql, param, cursor=cursor)
            if count >= 1:
                res = cursor.fetchall()
            else:
                res = []
            return res
        except:
            logger.info(f'[SAVE_LOG] {traceback.format_exc()}')
            return False

    def get_one(self, sql, param=None):
        '''

        :param sql:
        :param param:
        :return: 返回 dict
        '''
        conn = self.get_conn()
        cursor = conn.cursor()
        count = self.__query(sql, param, cursor=cursor)
        if count >= 1:
            res = cursor.fetchone()
        else:
            res = {}
        return res

    def get_many(self, sql, num, param=None):
        conn = self.get_conn()
        cursor = conn.cursor()
        count = self.__query(sql, param, cursor=cursor)
        if count >= 1:
            res = cursor.fetchmany(num)
        else:
            res = False
        return res

    def insert_one(self, sql, value=None):
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            if not value:
                cursor.execute(sql)
            else:
                cursor.execute(sql, value)
            return self.__getInsertId(cursor=cursor)
        # 捕获索引冲突异常
        except pymysql.err.IntegrityError as e:
            return 'duplicate'
        except:
            logger.info(f'[SAVE_LOG] {traceback.format_exc()}')
            return False

    def __getInsertId(self, cursor=None):
        t_cursor = self._cursor if not cursor else cursor
        t_cursor.execute('select @@IDENTITY AS id')
        result = t_cursor.fetchall()
        return result[0]['id']

    def insert_many(self, sql, values, cursor=None):
        '''
        使用原生executemany 批量插入数据
        :param sql: insert into table(task_id,name) values(%s,%s)
        :param values: 是一个列表，列表中的每一个元素必须是元组 [(1,'测试 1'),(2,'测试 2'),(3,'测试 3'),...]
        :return:
        '''
        assert isinstance(values, list), '批量插入 values 必须是 list'
        t_cursor = self._cursor if not cursor else cursor
        try:
            count = t_cursor.executemany(sql, values)
            return count
        except:
            logger.info(f'[SAVE_LOG] {traceback.format_exc()}')
            return False

    def __query(self, sql, param=None, cursor=None):
        """
        使用指定的游标进行查询，如果不入则使用默认的
        """
        t_cursor = self._cursor if not cursor else cursor
        if param is None:
            count = t_cursor.execute(sql)
        else:
            count = t_cursor.execute(sql, param)
        return count

    def query_cursor(self, sql):
        conn = self.get_conn()
        cursor = conn.cursor()
        count = cursor.execute(sql)
        return count

    def update(self, table, update_field: dict, update_by_field: dict):
        '''
        按条件更新某些字段
        :param table: 表名
        :param update_field: set 所需的字段, 必传
        :param update_by_field: where 所需的条件字段, 可为空
        :return: 影响行数
        '''
        set_list = list(
            map(lambda x: f"{x[0]}={x[1]}" if isinstance(x[1], int) else f"{x[0]}='{x[1]}'", update_field.items()))
        set_list = ','.join(set_list)
        # 有 where 条件
        if update_by_field:
            where_list = list(
                map(lambda x: f"{x[0]}={x[1]}" if isinstance(x[1], int) else f"{x[0]}='{x[1]}'",
                    update_by_field.items()))
            where_list = ' and '.join(where_list)
            sql = "UPDATE {table} SET {set_key} WHERE {where_key}".format(table=table, set_key=set_list,
                                                                          where_key=where_list)
        # 没有 where 条件
        else:
            sql = "UPDATE {table} SET {set_key} ".format(table=table, set_key=set_list)
        conn = self.get_conn()
        cursor = conn.cursor()
        count = cursor.execute(sql)
        return count

    def delete(self, sql, param=None):
        return self.__query(sql, param)

    def begin(self):
        # 开启事务
        self._conn.autocommit(0)

    def end(self, option='commit'):
        # 结束事务
        if option == 'commit':
            self._conn.commit()
        else:
            self._conn.rollback()

    def close(self, is_end=1):
        if is_end == 1:
            self.end('commit')
        else:
            self.end('rollback')
        self._cursor.close()
        self._conn.close()

    def update_ext(self, sql, value):
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            if not value:
                cursor.execute(sql)
            else:
                cursor.execute(sql, value)
            return self.__getInsertId(cursor=cursor)
        # 捕获索引冲突异常
        except pymysql.err.IntegrityError as e:
            return 'duplicate'
        except:
            logger.info(f'[SAVE_LOG] {traceback.format_exc()}')
            return False


if __name__ == '__main__':
    # 测试连接
    # mysql_client = MysqlPool('insight_crawler_news')
    import gzip
    from io import BytesIO

    mysql_client = MysqlPool()
    user_id = 'U72e5ab967b1246ba0b0e685cc5ae2895'
    text = 'jknkk'
    date = '2023-05-31 17:48:51'
    group_id = 'U72e5ab967b1246ba0b0e685cc5ae2895'
    mysql_client.insert_one('''insert ignore into line_dialogue (user_id, message, date, group_id) values
                            (%s, %s, %s, %s)
                ''', value=(user_id, text, date, group_id if group_id else ''))

