import datetime

from database.db import Role, docs_sales, docs_sales_delivery_info, docs_sales_links
from sqlalchemy import String, and_, cast, exists, func, or_


class FilterService:
    def apply_filters(self, filters, query):
        filters_dict = filters.dict(exclude_none=True)
        filter_list = []

        # ДОБАВЛЯЕМ JOIN если нужны фильтры доставки
        if self._needs_delivery_join(filters_dict):
            query = query.outerjoin(
                docs_sales_delivery_info,
                docs_sales_delivery_info.c.docs_sales_id == docs_sales.c.id,
            )

        # Обработка специальных фильтров
        self._handle_special_filters(filters_dict, filter_list)

        # Обработка общих фильтров по полям
        self._handle_general_filters(filters_dict, filter_list)

        return filter_list, query

    @staticmethod
    def _needs_delivery_join(filters_dict):
        """Проверяем, нужен ли JOIN к таблице доставки"""
        delivery_filters = {"delivery_date_from", "delivery_date_to", "has_delivery"}
        return any(field in filters_dict for field in delivery_filters)

    def _handle_special_filters(self, filters_dict, filter_list):
        """Обработка специальных фильтров (связи, доставка и т.д.)"""

        # Фильтры по ID сотрудников
        if "picker_id" in filters_dict:
            filter_list.append(
                self._create_employee_filter(filters_dict["picker_id"], Role.picker)
            )

        if "courier_id" in filters_dict:
            filter_list.append(
                self._create_employee_filter(filters_dict["courier_id"], Role.courier)
            )

        # Фильтры по дате доставки
        if "delivery_date_from" in filters_dict:
            filter_list.append(
                docs_sales_delivery_info.c.delivery_date
                >= datetime.datetime.fromtimestamp(filters_dict["delivery_date_from"])
            )

        if "delivery_date_to" in filters_dict:
            filter_list.append(
                docs_sales_delivery_info.c.delivery_date
                <= datetime.datetime.fromtimestamp(filters_dict["delivery_date_to"])
            )

        # Фильтры по наличию
        if "has_delivery" in filters_dict:
            filter_list.append(
                self._create_delivery_exists_filter(filters_dict["has_delivery"])
            )

        if "has_picker" in filters_dict:
            filter_list.append(
                self._create_role_exists_filter(filters_dict["has_picker"], Role.picker)
            )

        if "has_courier" in filters_dict:
            filter_list.append(
                self._create_role_exists_filter(
                    filters_dict["has_courier"], Role.courier
                )
            )

        # Фильтры по статусу и приоритету
        if "order_status" in filters_dict:
            filter_list.append(self._create_status_filter(filters_dict["order_status"]))

        if "priority" in filters_dict:
            filter_list.append(self._create_priority_filter(filters_dict["priority"]))

    @staticmethod
    def _create_employee_filter(employee_id, role):
        """Фильтр по ID сотрудника определенной роли"""
        return exists().where(
            and_(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == role,
                docs_sales_links.c.user_id == employee_id,
            )
        )

    def _create_delivery_exists_filter(self, has_delivery):
        """Фильтр по наличию доставки"""
        delivery_condition = self._get_delivery_validation_condition()
        delivery_exists = exists().where(
            and_(
                docs_sales_delivery_info.c.docs_sales_id == docs_sales.c.id,
                delivery_condition,
            )
        )
        return delivery_exists if has_delivery else ~delivery_exists

    @staticmethod
    def _get_delivery_validation_condition():
        """Условие валидации данных доставки"""
        address_valid = and_(
            docs_sales_delivery_info.c.address.isnot(None),
            func.trim(docs_sales_delivery_info.c.address) != "",
        )

        note_valid = and_(
            docs_sales_delivery_info.c.note.isnot(None),
            func.trim(docs_sales_delivery_info.c.note) != "",
        )

        delivery_date_valid = docs_sales_delivery_info.c.delivery_date.isnot(None)

        recipient_text = func.trim(cast(docs_sales_delivery_info.c.recipient, String))
        recipient_valid = and_(
            docs_sales_delivery_info.c.recipient.isnot(None),
            recipient_text != "",
            recipient_text != "{}",
            recipient_text != "null",
            recipient_text != "[]",
        )

        return or_(address_valid, note_valid, delivery_date_valid, recipient_valid)

    @staticmethod
    def _create_role_exists_filter(has_role, role):
        """Фильтр по наличию роли"""
        role_exists = exists().where(
            and_(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == role,
            )
        )
        return role_exists if has_role else ~role_exists

    @staticmethod
    def _create_status_filter(order_status):
        """Фильтр по статусу заказа"""
        statuses = [s.strip() for s in order_status.split(",") if s.strip()]
        return docs_sales.c.order_status.in_(statuses) if statuses else None

    @staticmethod
    def _create_priority_filter(priority):
        """Фильтр по приоритету"""
        if isinstance(priority, dict):
            conditions = []
            for op, val in priority.items():
                if op == "gt":
                    conditions.append(docs_sales.c.priority > val)
                if op == "lt":
                    conditions.append(docs_sales.c.priority < val)
                if op == "eq":
                    conditions.append(docs_sales.c.priority == val)
            return and_(*conditions) if conditions else None
        else:
            return docs_sales.c.priority == priority

    def _handle_general_filters(self, filters_dict, filter_list):
        """Обработка общих фильтров по полям документа"""
        excluded_keys = {
            "has_delivery",
            "has_picker",
            "has_courier",
            "priority",
            "order_status",
            "delivery_date_from",
            "delivery_date_to",
            "picker_id",
            "courier_id",
        }

        for field, value in filters_dict.items():
            if field in excluded_keys:
                continue
            # Поддержка старого формата *_from / *_to
            if field.endswith("_from") or field.endswith("_to"):
                self._handle_date_filter(field, value, filters_dict, filter_list)
                continue

            # Поддержка нового формата field__gte / field__lte / и т.д.
            if "__" in field:
                base_field, op = field.split("__", 1)
                self._handle_operator_filter(base_field, op, value, filter_list)
                continue

            # Обычный фильтр (подстрочный поиск или равенство)
            self._handle_regular_filter(field, value, filter_list)

    @staticmethod
    def _handle_date_filter(field, value, filters_dict, filter_list):
        """Обработка фильтров по дате"""
        if field.endswith("_from"):
            base_field = field.replace("_from", "")
            to_field = field.replace("from", "to")
            to_value = filters_dict.get(to_field)

            if to_value:
                # Диапазон дат
                filter_list.append(
                    and_(
                        func.to_timestamp(value) <= getattr(docs_sales.c, base_field),
                        getattr(docs_sales.c, base_field)
                        <= func.to_timestamp(to_value),
                    )
                )
            else:
                # Только от даты
                filter_list.append(
                    getattr(docs_sales.c, base_field) >= func.to_timestamp(value)
                )

        # _to обрабатывается в _from фильтре, поэтому пропускаем

    @staticmethod
    def _handle_regular_filter(field, value, filter_list):
        """Обработка обычных фильтров"""
        field_ref = getattr(docs_sales.c, field)

        if isinstance(value, bool):
            filter_list.append(field_ref.is_(value))

        elif isinstance(value, str):
            if "," in value:
                # Множественные значения
                values = [v.strip() for v in value.split(",") if v.strip()]
                conditions = [field_ref.ilike(f"%{v}%") for v in values]
                filter_list.append(or_(*conditions))
            else:
                # Одиночное значение
                filter_list.append(field_ref.ilike(f"%{value.strip()}%"))

        else:
            # Числовые и другие значения
            filter_list.append(field_ref == value)

    @staticmethod
    def _handle_operator_filter(field, op, value, filter_list):
        """Обработка фильтров с операторами в стиле field__gte=..."""
        if not hasattr(docs_sales.c, field):
            # Игнорируем неизвестные поля чтобы не падать, можно логировать
            return
        field_ref = getattr(docs_sales.c, field)

        # Если значение timestamp (int) и колонка типа DateTime — преобразуем
        if (
            isinstance(value, int)
            and getattr(getattr(field_ref, "type", None), "__class__", None).__name__
            == "DateTime"
        ):
            value_converted = func.to_timestamp(value)
        else:
            value_converted = value

        op_map = {
            "gte": lambda c, v: c >= v,
            "lte": lambda c, v: c <= v,
            "gt": lambda c, v: c > v,
            "lt": lambda c, v: c < v,
            "ne": lambda c, v: c != v,
            "eq": lambda c, v: c == v,
            "in": lambda c, v: c.in_(
                v if isinstance(v, (list, tuple)) else str(v).split(",")
            ),
        }

        handler = op_map.get(op)
        if handler is None:
            # Неподдерживаемый оператор — пропускаем (можно логировать)
            return

        filter_list.append(handler(field_ref, value_converted))
