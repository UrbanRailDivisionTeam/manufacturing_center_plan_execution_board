# ============================================================
# 导入模块
# ============================================================
from pathlib import Path  # 用于处理文件路径
import mimetypes  # 用于处理文件的MIME类型
import pandas as pd
import clickhouse_connect  # ClickHouse数据库连接驱动
import clickhouse_connect.driver.asyncclient
import pandas as pd
from clickhouse_connect.driver import httputil
from litestar import Litestar, get  # Litestar Web框架
from litestar.static_files.config import StaticFilesConfig  # 静态文件配置
from litestar.response import Response  # 响应对象
from datetime import datetime, time, timedelta, date  # 日期时间处理
import holidays  # 中国法定节假日
import asyncio
import warnings

# ============================================================
# 1. 配置部分
# ============================================================

mimetypes.add_type("application/javascript", ".js")
warnings.filterwarnings("ignore", category=FutureWarning)


# ============================================================
# 2. 工具函数部分
# ============================================================
from contextlib import asynccontextmanager

_client = None


async def get_client() -> clickhouse_connect.driver.asyncclient.AsyncClient:
    global _client
    if _client is None:
        pool_mgr = httputil.get_pool_manager(
            maxsize=32,
            num_pools=2,
            block=False,  # 关键：池满时不丢弃连接，而是新建临时连接
            timeout=150,
        )
        _client = await clickhouse_connect.get_async_client(
            host="10.24.5.59",
            port=8123,
            username="cheakf",
            password="Swq8855830.",
            database="default",
            pool_mgr=pool_mgr,
        )
    return _client


def format_time(dt):
    """
    格式化时间显示
    - 将datetime/time对象格式化为 'MM-DD HH:MM' 格式
    - 处理1970年1月1日的无效日期（返回空字符串）
    - 处理空值情况（返回 '--:--'）
    """
    if dt is None:
        return "--:--"
    if isinstance(dt, datetime):
        # 1970年1月1日是ClickHouse中的默认空值，返回空字符串
        if dt.year == 1970 and dt.month == 1 and dt.day == 1:
            return ""
        return dt.strftime("%m-%d %H:%M")
    elif isinstance(dt, time):
        return dt.strftime("%H:%M")
    try:
        # 尝试解析ISO格式字符串
        dt_iso = datetime.fromisoformat(str(dt))
        if dt_iso.year == 1970 and dt_iso.month == 1 and dt_iso.day == 1:
            return ""
        return dt_iso.strftime("%m-%d %H:%M")
    except (ValueError, TypeError):
        return "--:--"


def get_workdays_in_last_n_days(n=7):
    """
    获取近N个工作日日期（排除法定节假日和周末）
    返回包含N个工作日的列表
    """
    cn_holidays = holidays.CN(years=range(2020, 2030))  # type: ignore
    workdays = []
    current_date = date.today()

    while len(workdays) < n:
        # 检查是否是节假日或周末（周六=5，周日=6）
        if current_date not in cn_holidays and current_date.weekday() < 5:
            workdays.append(current_date)
        current_date -= timedelta(days=1)

    workdays.reverse()
    return workdays


# ============================================================
# 3. API 接口部分
# ============================================================

ASSEMBLY_TEAMS = [
    "车内电工班",
    "车外电工班",
    "预组装班",
    "车内钳工班",
    "车外钳工班",
    "粘接综合班",
    "线槽地板班",
    "内装一班",
    "内装二班",
    "调车班",
    "质检班",
    "设备工位",
    "车门工位",
    "辅助工位",
]

DELIVERY_TEAMS = [
    "校线一班",
    "校线二班",
    "调试一班",
    "调试二班",
    "调试三班",
    "调试四班",
    "落车班",
]


def map_status(table_status):
    """将表中的工序状态映射为前端期望的状态"""
    status_mapping = {
        "待开工": "待开工",
        "已完工": "已完成",
        "执行中": "执行中",
        "已超时": "已超时",
        "错误-无法判断状态": "待开工",
    }
    return status_mapping.get(table_status, "待开工")


def get_status_info(status):
    """根据状态返回 is_overtime 和 is_pending 标志"""
    return {
        "待开工": (False, True),
        "执行中": (False, False),
        "已完成": (False, False),
        "已超时": (True, False),
    }.get(status, (False, False))


def build_filter(team, assembly, delivery):
    """构建班组过滤条件"""
    if team:
        return f"AND BILL.`班组名称` = '{team}'"
    if assembly:
        return f"AND BILL.`班组名称` IN {tuple(ASSEMBLY_TEAMS)}"
    if delivery:
        return f"AND BILL.`班组名称` IN {tuple(DELIVERY_TEAMS)}"
    return ""


@get("/api/table-data")
async def get_table_data(
    team: str | None = None, assembly: bool = False, delivery: bool = False
) -> dict:
    """
    获取表格数据的主API接口
    参数:
        - team: 班组名称过滤（控制整个页面的所有数据）
        - assembly: 是否只获取总成车间数据
        - delivery: 是否只获取交车车间数据
    返回:
        - table_data: 今日执行队列数据
        - summary: 汇总统计数据
    """
    try:
        client = await get_client()
        team_filter = build_filter(team, assembly, delivery)

        query = f"""
            SELECT
                BILL.`项目号`,
                BILL.`车号`,
                BILL.`节车号`,
                BILL.`工序编码`,
                BILL.`工序名称`,
                BILL.`排程开始时间`,
                BILL.`排程结束时间`,
                BILL.`计划开始时间`,
                BILL.`计划结束时间`,
                BILL.`实际开始时间`,
                BILL.`实际结束时间`,
                BILL.`班组名称`,
                BILL.`是否兑现节拍`,
                BILL.`是否准时开完工`,
                BILL.`当前工序状态`,
                BILL.`排程执行时间`,
                BILL.`计划执行时间`,
                BILL.`实际执行时间`
            FROM
                dwd.beat_fulfillment_rate BILL
            WHERE
                ((toStartOfMonth(toDate(BILL.`计划开始时间`)) = toStartOfMonth(today())
                OR toDate(BILL.`实际结束时间`) = today()
                OR (toDate(BILL.`计划开始时间`) >= today() - INTERVAL 6 DAY AND toDate(BILL.`计划开始时间`) <= today()))
                {team_filter})
            ORDER BY
                BILL.`计划开始时间` DESC
        """
        trend_query = f"""
            SELECT
                toDate(BILL.`计划开始时间`) as plan_date,
                COUNT(*) as total,
                sum(if(BILL.`是否兑现节拍` = '是', 1, 0)) as beat_ok
            FROM
                dwd.beat_fulfillment_rate BILL
            WHERE
                toDate(BILL.`计划开始时间`) >= today() - INTERVAL 6 DAY
                AND toDate(BILL.`计划开始时间`) <= today()
                {team_filter}
            GROUP BY toDate(BILL.`计划开始时间`)
            ORDER BY plan_date
        """
        ontime_trend_query = f"""
            SELECT
                toDate(BILL.`计划开始时间`) as plan_date,
                COUNT(*) as total,
                sum(if(BILL.`是否准时开完工` = '是', 1, 0)) as on_time_ok
            FROM
                dwd.beat_fulfillment_rate BILL
            WHERE
                toDate(BILL.`计划开始时间`) >= today() - INTERVAL 6 DAY
                AND toDate(BILL.`计划开始时间`) <= today()
                {team_filter}
            GROUP BY toDate(BILL.`计划开始时间`)
            ORDER BY plan_date
        """
        df, trend_df, ontime_trend_df = await asyncio.gather(
            client.query_df(query),
            client.query_df(trend_query),
            client.query_df(ontime_trend_query),
        )

        local_today = date.today()

        df["计划开始时间"] = pd.to_datetime(
            df["计划开始时间"], errors="coerce"
        ).dt.tz_localize(None)
        df["计划结束时间"] = pd.to_datetime(
            df["计划结束时间"], errors="coerce"
        ).dt.tz_localize(None)
        df["实际开始时间"] = pd.to_datetime(
            df["实际开始时间"], errors="coerce"
        ).dt.tz_localize(None)
        df["实际结束时间"] = pd.to_datetime(
            df["实际结束时间"], errors="coerce"
        ).dt.tz_localize(None)

        df["is_today"] = df["计划开始时间"].dt.date == local_today
        df["is_month"] = (df["计划开始时间"].dt.year == local_today.year) & (
            df["计划开始时间"].dt.month == local_today.month
        )
        df["is_finished_today"] = df["实际结束时间"].dt.date == local_today
        df["plan_date"] = df["计划开始时间"].dt.date

        status_counts = df[df["is_today"]]["当前工序状态"].value_counts().to_dict()
        status_counts = {map_status(k): v for k, v in status_counts.items()}

        today_scheduled = int(df["is_today"].sum())
        today_completed = int(df["is_finished_today"].sum())

        month_df = df[df["is_month"]]
        month_total = len(month_df)
        month_beat_ok = int((month_df["是否兑现节拍"] == "是").sum())
        month_on_time_ok = int((month_df["是否准时开完工"] == "是").sum())

        today_df = df[df["is_today"]]
        today_beat_ok = int((today_df["是否兑现节拍"] == "是").sum())
        today_on_time_ok = int((today_df["是否准时开完工"] == "是").sum())

        workdays = get_workdays_in_last_n_days(7)
        trend_days_dict = {
            d: {"total": 0, "beat_ok": 0, "on_time_ok": 0} for d in workdays
        }

        for _, row in trend_df.iterrows():
            p_date = (
                row["plan_date"].date()
                if hasattr(row["plan_date"], "date")
                else row["plan_date"]
            )
            if p_date in trend_days_dict:
                trend_days_dict[p_date]["total"] = row["total"]
                trend_days_dict[p_date]["beat_ok"] = row["beat_ok"]

        last_7_days_beat = []
        for d in sorted(trend_days_dict.keys()):
            day_data = trend_days_dict[d]
            beat_rate = (
                round((day_data["beat_ok"] / day_data["total"] * 100), 1)
                if day_data["total"] > 0
                else 0
            )
            last_7_days_beat.append({"date": d.strftime("%m-%d"), "rate": beat_rate})

        last_7_days_on_time = []
        if team:
            ontime_days_dict = {d: {"total": 0, "on_time_ok": 0} for d in workdays}
            for _, row in ontime_trend_df.iterrows():
                p_date = (
                    row["plan_date"].date()
                    if hasattr(row["plan_date"], "date")
                    else row["plan_date"]
                )
                if p_date in ontime_days_dict:
                    ontime_days_dict[p_date]["total"] = row["total"]
                    ontime_days_dict[p_date]["on_time_ok"] = row["on_time_ok"]
            for d in sorted(ontime_days_dict.keys()):
                day_data = ontime_days_dict[d]
                on_time_rate = (
                    round((day_data["on_time_ok"] / day_data["total"] * 100), 1)
                    if day_data["total"] > 0
                    else 0
                )
                last_7_days_on_time.append(
                    {"date": d.strftime("%m-%d"), "rate": on_time_rate}
                )
        else:
            days_grouped = df.groupby("plan_date").agg(
                total=("项目号", "count"),
                beat_ok=("是否兑现节拍", lambda x: (x == "是").sum()),
                on_time_ok=("是否准时开完工", lambda x: (x == "是").sum()),
            )
            for d in workdays:
                if d in days_grouped.index:
                    day_data = days_grouped.loc[d]
                    on_time_rate = (
                        round((day_data["on_time_ok"] / day_data["total"] * 100), 1)
                        if day_data["total"] > 0
                        else 0
                    )
                    last_7_days_on_time.append(
                        {"date": d.strftime("%m-%d"), "rate": on_time_rate}
                    )

        status_map = {
            "已超时": "bg-primary-container text-white",
            "执行中": "bg-[#00C853] text-white",
            "待开工": "bg-surface-container-highest text-on-surface",
            "已完成": "bg-[#00C853] text-white",
        }

        table_data = []
        today_rows = df[df["is_today"]].copy()
        for _, row in today_rows.iterrows():
            status = map_status(row["当前工序状态"])
            is_overtime, is_pending = get_status_info(status)

            plan_start = row["计划开始时间"]
            plan_end = row["计划结束时间"]
            actual_start = row["实际开始时间"]
            actual_end = row["实际结束时间"]

            scheduled_duration = (
                f"{int(row['计划执行时间'])}M"
                if pd.notna(row.get("计划执行时间")) and row["计划执行时间"] > 0
                else "--"
            )
            execution_duration = (
                f"{int(row['实际执行时间'])}M"
                if pd.notna(row.get("实际执行时间")) and row["实际执行时间"] > 0
                else "--"
            )

            table_data.append(
                {
                    "status": status,
                    "status_class": status_map.get(status, ""),
                    "project": row["项目号"],
                    "train_no": row["车号"],
                    "car_no": row["节车号"],
                    "process_code": row["工序编码"],
                    "process_name": row["工序名称"],
                    "plan_start": format_time(plan_start),
                    "plan_end": format_time(plan_end),
                    "actual_start": format_time(actual_start),
                    "actual_end": format_time(actual_end),
                    "scheduled_duration": scheduled_duration,
                    "execution_duration": execution_duration,
                    "is_overtime": is_overtime,
                    "is_pending": is_pending,
                }
            )

        month_beat_rate = (
            round((month_beat_ok / month_total * 100), 1) if month_total > 0 else 0
        )
        month_on_time_rate = (
            round((month_on_time_ok / month_total * 100), 1) if month_total > 0 else 0
        )
        today_beat_rate = (
            round((today_beat_ok / today_scheduled * 100), 1)
            if today_scheduled > 0
            else 0
        )
        today_on_time_rate = (
            round((today_on_time_ok / today_scheduled * 100), 1)
            if today_scheduled > 0
            else 0
        )
        today_remaining = max(0, today_scheduled - today_completed)

        return {
            "table_data": table_data,
            "summary": {
                "total_count": len(table_data),
                "overdue": status_counts.get("已超时", 0),
                "in_progress": status_counts.get("执行中", 0),
                "pending": status_counts.get("待开工", 0),
                "today_scheduled": today_scheduled,
                "today_completed": today_completed,
                "today_remaining": today_remaining,
                "month_beat_rate": month_beat_rate,
                "month_on_time_rate": month_on_time_rate,
                "today_beat_rate": today_beat_rate,
                "today_on_time_rate": today_on_time_rate,
                "last_7_days_beat": last_7_days_beat,
                "last_7_days_on_time": last_7_days_on_time,
            },
        }
    except Exception as e:
        print(f"Error fetching data from ClickHouse: {e}")
        return {
            "table_data": [],
            "summary": {
                "total_count": 0,
                "overdue": 0,
                "in_progress": 0,
                "pending": 0,
                "today_scheduled": 0,
                "today_completed": 0,
                "today_remaining": 0,
            },
        }


@get("/api/team-ranking")
async def get_team_ranking() -> dict:
    """
    获取本月各班组的节拍兑现率和报工准时率排名
    返回倒数前7名班组
    """
    try:
        client = await get_client()

        all_teams = ASSEMBLY_TEAMS + DELIVERY_TEAMS

        beat_ranking_query = f"""
            SELECT
                BILL.`班组名称` as team_name,
                COUNT(*) as total,
                sum(if(BILL.`是否兑现节拍` = '是', 1, 0)) as beat_ok
            FROM
                dwd.beat_fulfillment_rate BILL
            WHERE
                toStartOfMonth(toDate(BILL.`计划开始时间`)) = toStartOfMonth(today())
                AND BILL.`班组名称` IN {tuple(all_teams)}
            GROUP BY BILL.`班组名称`
            HAVING total > 0
            ORDER BY beat_ok / total ASC
            LIMIT 7
        """
        on_time_ranking_query = f"""
            SELECT
                BILL.`班组名称` as team_name,
                COUNT(*) as total,
                sum(if(BILL.`是否准时开完工` = '是', 1, 0)) as on_time_ok
            FROM
                dwd.beat_fulfillment_rate BILL
            WHERE
                toStartOfMonth(toDate(BILL.`计划开始时间`)) = toStartOfMonth(today())
                AND BILL.`班组名称` IN {tuple(all_teams)}
            GROUP BY BILL.`班组名称`
            HAVING total > 0
            ORDER by on_time_ok / total ASC
            LIMIT 7
        """
        beat_df, on_time_df = await asyncio.gather(
            client.query_df(beat_ranking_query),
            client.query_df(on_time_ranking_query),
        )

        beat_df["rate"] = beat_df.apply(
            lambda r: round((r["beat_ok"] / r["total"] * 100), 1)
            if r["total"] > 0
            else 0,
            axis=1,
        )
        on_time_df["rate"] = on_time_df.apply(
            lambda r: round((r["on_time_ok"] / r["total"] * 100), 1)
            if r["total"] > 0
            else 0,
            axis=1,
        )

        beat_ranking = beat_df[["team_name", "rate"]].to_dict("records")
        on_time_ranking = on_time_df[["team_name", "rate"]].to_dict("records")

        return {"beat_ranking": beat_ranking, "on_time_ranking": on_time_ranking}
    except Exception as e:
        print(f"Error fetching team ranking: {e}")
        return {"beat_ranking": [], "on_time_ranking": []}


# ============================================================
# 4. 静态页面路由
# ============================================================


@get("/favicon.ico")
async def favicon() -> Response:
    """favicon.ico 图标路由"""
    icon_path = Path("static/favicon.ico")
    return Response(content=icon_path.read_bytes(), media_type="image/x-icon")


@get("/")
async def index_html() -> Response:
    """根路径路由，返回静态HTML页面"""
    html_path = Path("static/index.html")
    html_content = html_path.read_text(encoding="utf-8")
    return Response(content=html_content, media_type="text/html")


# ============================================================
# 5. 应用启动配置
# ============================================================

app = Litestar(
    route_handlers=[index_html, favicon, get_table_data, get_team_ranking],  # 注册路由处理器
    static_files_config=[
        StaticFilesConfig(
            path="/static",  # URL路径前缀
            directories=["static"],  # 静态文件目录
            name="static",
        )
    ],
)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=12383)
