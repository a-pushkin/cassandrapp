#pragma once

#include <chrono>
#include <cstdint>
#include <exception>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>
#include "cassandra.h"

namespace cassandrapp {

class error : public std::exception {
 public:
  static void check_and_throw(CassError err, CassFuture* future = nullptr) {
    if (err != CASS_OK) {
      std::string msg{"Cassandra error: "};
      if (future != nullptr) {
        const char* message = nullptr;
        size_t message_len{0};
        cass_future_error_message(future, &message, &message_len);
        if (message_len) {
          msg = std::string(message, message_len);
        }
      } else {
        msg += cass_error_desc(err);
      }

      throw error(err, msg);
    }
  }

  error(CassError _err, const std::string msg = "") : err(_err), msg_{msg} {}

  const CassError err;

  virtual const char* what() const throw() override { return msg_.c_str(); }

 private:
  mutable std::string msg_;
};

class value final {
 public:
  value(const CassValue_* data) : data_{data} {}

  int8_t get_int8() const {
    int8_t result;
    error::check_and_throw(cass_value_get_int8(data_, &result));
    return result;
  }

  int16_t get_int16() const {
    int16_t result;
    error::check_and_throw(cass_value_get_int16(data_, &result));
    return result;
  }

  int32_t get_int32() const {
    int32_t result;
    error::check_and_throw(cass_value_get_int32(data_, &result));
    return result;
  }

  uint32_t get_uint32() const {
    uint32_t result;
    error::check_and_throw(cass_value_get_uint32(data_, &result));
    return result;
  }

  int64_t get_int64() const {
    int64_t result;
    error::check_and_throw(cass_value_get_int64(data_, &result));
    return result;
  }

  std::chrono::system_clock::time_point get_timestamp() const {
    return std::chrono::system_clock::time_point{
        std::chrono::microseconds{get_int64()}};
  }

  /**
   * TODO: Should probably be named as_float() and have explicit cast operator;
   */
  float get_float() const {
    float result;
    error::check_and_throw(cass_value_get_float(data_, &result));
    return result;
  }

  double get_double() const {
    double result;
    error::check_and_throw(cass_value_get_double(data_, &result));
    return result;
  }

  std::string get_string() const {
    const char* buffer;
    size_t buffer_size;
    error::check_and_throw(cass_value_get_string(data_, &buffer, &buffer_size));
    return std::string{buffer, buffer_size};
  }

  std::vector<uint8_t> get_bytes() const {
    const uint8_t* buffer;
    size_t buffer_size;
    error::check_and_throw(cass_value_get_bytes(data_, &buffer, &buffer_size));
    return std::vector<uint8_t>{buffer, buffer + buffer_size};
  }

  void get_bytes(std::vector<uint8_t>& bytes) const {
    const uint8_t* buffer;
    size_t buffer_size;
    error::check_and_throw(cass_value_get_bytes(data_, &buffer, &buffer_size));
    bytes.resize(buffer_size);
    std::copy(buffer, buffer + buffer_size, std::begin(bytes));
  }

  bool is_null() const { return cass_value_is_null(data_); }

  CassValueType type() const { return cass_value_type(data_); }

 private:
  const CassValue_* data_;
};

class row final {
 public:
  row(const CassRow_* data) : data_{data} {}

  value get_column(size_t index) const {
    const auto* data = cass_row_get_column(data_, index);
    if (data == nullptr) {
      throw error{CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS};
    } else {
      return value{data};
    }
  }

 private:
  const CassRow_* data_;
};

class result_iterator final {
 public:
  result_iterator(CassIterator* data) : data_{data} {
    if (data_ != nullptr) {
      ++(*this);
    }
  }

  result_iterator(const result_iterator&) = delete;
  result_iterator& operator=(const result_iterator&) = delete;

  result_iterator(result_iterator&& other) { *this = std::move(other); }

  result_iterator& operator=(result_iterator&& other) {
    dispose();
    data_ = other.data_;
    curr_row_ = other.curr_row_;
    other.data_ = nullptr;

    return *this;
  }

  ~result_iterator() { dispose(); }

  result_iterator& operator++() {
    if (!cass_iterator_next(data_)) {
      dispose();
    } else {
      curr_row_ = row{cass_iterator_get_row(data_)};
    }

    return *this;
  }

  bool operator!=(const result_iterator& other) const {
    return data_ != other.data_;
  }

  bool operator==(const result_iterator& other) const {
    return data_ == other.data_;
  }

  const row& operator*() const { return curr_row_; }

  void dispose() {
    if (data_ != nullptr) {
      cass_iterator_free(data_);
      data_ = nullptr;
    }
  }

 private:
  CassIterator* data_{nullptr};
  row curr_row_{nullptr};
};

class result final {
 public:
  result(const CassResult_* data) : data_(data) {}

  ~result() { dispose(); }

  void dispose() {
    if (data_ != nullptr) {
      cass_result_free(data_);
    }
  }

  result(result&& other) { *this = std::move(other); }

  bool has_more_pages() const { return cass_result_has_more_pages(data_); }

  size_t row_count() const { return cass_result_row_count(data_); }

  size_t column_count() const { return cass_result_column_count(data_); }

  CassValueType column_type(size_t index) const {
    return cass_result_column_type(data_, index);
  }

  std::string column_name(size_t index) const {
    const char* buffer;
    size_t buffer_size;
    error::check_and_throw(
        cass_result_column_name(data_, index, &buffer, &buffer_size));
    return std::string{buffer, buffer_size};
  }

  row first_row() const {
    const auto* data = cass_result_first_row(data_);
    if (data == nullptr) {
      throw error{CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS};
    } else {
      return row{data};
    }
  }

  result_iterator begin() const {
    return result_iterator{cass_iterator_from_result(data_)};
  }

  result_iterator end() const { return result_iterator{nullptr}; }

  result& operator=(result&& other) {
    dispose();
    data_ = other.data_;
    other.data_ = nullptr;
    return *this;
  }

  const CassResult* data() const { return data_; }

 private:
  result(const result&) = delete;
  result& operator=(const result&) = delete;

  const CassResult* data_{nullptr};
};

class future {
 public:
  future(CassFuture_* data, bool owns_result = false)
      : data_(data), owns_result_(owns_result) {}

  ~future() { dispose(); }

  void dispose() {
    if (data_ != nullptr) {
      if (owns_result_) {
        result(cass_future_get_result(data_));
      }

      cass_future_free(data_);
    }
  }

  future(future&& other) { *this = std::move(other); }

  future& operator=(future&& other) {
    dispose();
    data_ = other.data_;
    other.data_ = nullptr;
    other.owns_result_ = false;
    return *this;
  }

  void wait() const {
    cass_future_wait(data_);
    error::check_and_throw(cass_future_error_code(data_), data_);
  }

  bool wait_timed(cass_duration_t wait_ns) const {
    return cass_future_wait_timed(data_, wait_ns);
  }

  result get_result() {
    wait();
    owns_result_ = false;
    return result(cass_future_get_result(data_));
  }

  CassFuture* data() const { return data_; }

 private:
  future(const future&) = delete;
  future& operator=(const future&) = delete;

  mutable CassFuture* data_{nullptr};
  bool owns_result_{false};
};

class statement {
 public:
  statement() : data_{nullptr} {}
  statement(CassStatement* data) : data_{data} {}

  statement(const char* query, int param_count = 0)
      : data_{cass_statement_new(query, param_count)} {}

  statement(statement&& other) { *this = std::move(other); }

  statement& operator=(statement&& other) {
    dispose();
    data_ = other.data_;
    other.data_ = nullptr;
    return *this;
  }

  statement& set_paging_size(int page_size) {
    error::check_and_throw(cass_statement_set_paging_size(data_, page_size));
    return *this;
  }

  statement& set_paging_state(const result& result) {
    error::check_and_throw(
        cass_statement_set_paging_state(data_, result.data()));
    return *this;
  }

  void reset_parameters(size_t count) {
    error::check_and_throw(cass_statement_reset_parameters(data_, count));
  }

  void add_key_index(size_t index) {
    error::check_and_throw(cass_statement_add_key_index(data_, index));
  }

  void set_keyspace(const char* keyspace) {
    error::check_and_throw(cass_statement_set_keyspace(data_, keyspace));
  }

  statement& bind_null(size_t index) {
    error::check_and_throw(cass_statement_bind_null(data_, index));
    return *this;
  }

  statement& bind_int8(size_t index, int8_t value) {
    error::check_and_throw(cass_statement_bind_int8(data_, index, value));
    return *this;
  }

  statement& bind_int16(size_t index, int16_t value) {
    error::check_and_throw(cass_statement_bind_int16(data_, index, value));
    return *this;
  }

  statement& bind_int32(size_t index, int32_t value) {
    error::check_and_throw(cass_statement_bind_int32(data_, index, value));
    return *this;
  }

  statement& bind_uint32(size_t index, uint32_t value) {
    error::check_and_throw(cass_statement_bind_uint32(data_, index, value));
    return *this;
  }

  statement& bind_int64(size_t index, int64_t value) {
    error::check_and_throw(cass_statement_bind_int64(data_, index, value));
    return *this;
  }

  statement& bind_timestamp(size_t index,
                            std::chrono::system_clock::time_point value) {
    int64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     value.time_since_epoch())
                     .count();
    error::check_and_throw(cass_statement_bind_int64(data_, index, ms));
    return *this;
  }

  statement& bind_float(size_t index, float value) {
    error::check_and_throw(cass_statement_bind_float(data_, index, value));
    return *this;
  }

  statement& bind_double(size_t index, double value) {
    error::check_and_throw(cass_statement_bind_double(data_, index, value));
    return *this;
  }

  statement& bind_bool(size_t index, bool value) {
    error::check_and_throw(
        cass_statement_bind_bool(data_, index, value ? cass_true : cass_false));
    return *this;
  }

  statement& bind_string(size_t index, const char* value) {
    error::check_and_throw(cass_statement_bind_string(data_, index, value));
    return *this;
  }

  statement& bind_string(size_t index, const std::string& value) {
    error::check_and_throw(
        cass_statement_bind_string_n(data_, index, value.data(), value.size()));
    return *this;
  }

  template <typename TArray>
  statement& bind_bytes(size_t index, const TArray& value) {
    error::check_and_throw(
        cass_statement_bind_bytes(data_, index, value.data(), value.size()));
    return *this;
  }

  template <typename TArray>
  statement& bind_custom(size_t index, const TArray& value) {
    error::check_and_throw(cass_statement_bind_custom(
        data_, index, &std::begin(value), std::end(value) - std::begin(value)));
    return *this;
  }

  statement& bind_null_by_name(const char* name) {
    error::check_and_throw(cass_statement_bind_null_by_name(data_, name));
    return *this;
  }

  statement& bind_int8_by_name(const char* name, int8_t value) {
    error::check_and_throw(
        cass_statement_bind_int8_by_name(data_, name, value));
    return *this;
  }

  statement& bind_int16_by_name(const char* name, int16_t value) {
    error::check_and_throw(
        cass_statement_bind_int16_by_name(data_, name, value));
    return *this;
  }

  statement& bind_int32_by_name(const char* name, int32_t value) {
    error::check_and_throw(
        cass_statement_bind_int32_by_name(data_, name, value));
    return *this;
  }

  statement& bind_uint32_by_name(const char* name, uint32_t value) {
    error::check_and_throw(
        cass_statement_bind_uint32_by_name(data_, name, value));
    return *this;
  }

  statement& bind_int64_by_name(const char* name, int64_t value) {
    error::check_and_throw(
        cass_statement_bind_int64_by_name(data_, name, value));
    return *this;
  }

  statement& bind_float_by_name(const char* name, float value) {
    error::check_and_throw(
        cass_statement_bind_float_by_name(data_, name, value));
    return *this;
  }

  statement& bind_double_by_name(const char* name, double value) {
    error::check_and_throw(
        cass_statement_bind_double_by_name(data_, name, value));
    return *this;
  }

  statement& bind_bool_by_name(const char* name, bool value) {
    error::check_and_throw(cass_statement_bind_bool_by_name(
        data_, name, value ? cass_true : cass_false));
    return *this;
  }

  statement& bind_string_by_name(const char* name, const char* value) {
    error::check_and_throw(
        cass_statement_bind_string_by_name(data_, name, value));
    return *this;
  }

  template <typename TArray>
  statement& bind_bytes_by_name(const char* name, const TArray& value) {
    error::check_and_throw(cass_statement_bind_bytes_by_name(
        data_, name, &std::begin(value), std::end(value) - std::begin(value)));
    return *this;
  }

  template <typename TArray>
  statement& bind_custom_by_name(const char* name, const TArray& value) {
    error::check_and_throw(cass_statement_bind_custom_by_name(
        data_, name, &std::begin(value), std::end(value) - std::begin(value)));
    return *this;
  }

  ~statement() { dispose(); }

  void dispose() {
    if (data_ != nullptr) {
      cass_statement_free(data_);
    }
  }

  CassStatement* data() const { return data_; }

 private:
  statement(const statement&) = delete;
  statement& operator=(const statement&) = delete;

  CassStatement* data_{nullptr};
};

class batch {
 public:
  batch(CassBatchType type = CASS_BATCH_TYPE_LOGGED)
      : data_{cass_batch_new(type)} {}

  batch(batch&& other) { *this = std::move(other); }

  batch& operator=(batch&& other) {
    dispose();
    data_ = other.data_;
    size_ = other.size_;
    other.data_ = nullptr;
    other.size_ = 0;
    return *this;
  }

  ~batch() { dispose(); }

  void dispose() {
    if (data_ != nullptr) {
      cass_batch_free(data_);
    }
  }

  size_t add_statement(const statement& s) {
    error::check_and_throw(cass_batch_add_statement(data_, s.data()));
    return ++size_;
  }

  CassBatch* data() const { return data_; }

  size_t size() const { return size_; }

 private:
  batch(const batch&) = delete;
  batch& operator=(const batch&) = delete;

  mutable CassBatch* data_{nullptr};
  size_t size_{0};
};

class prepared {
  friend class session;

 public:
  prepared(prepared&& other) { *this = std::move(other); }

  prepared& operator=(prepared&& other) {
    dispose();
    data_ = other.data_;
    other.data_ = nullptr;
    return *this;
  }

  ~prepared() { dispose(); }

  void dispose() {
    if (data_ != nullptr) {
      cass_prepared_free(data_);
    }
  }

  statement bind() const { return statement{cass_prepared_bind(data_)}; }

 private:
  prepared(CassSession* s, const char* query) {
    future f{cass_session_prepare(s, query)};
    f.wait();
    data_ = cass_future_get_prepared(f.data());
  }

  prepared(const prepared&) = delete;
  prepared& operator=(const prepared&) = delete;

  const CassPrepared* data_{nullptr};
};

class session {
  friend class cluster;

 public:
  void dispose() {
    if (data_ != nullptr) {
      cass_session_free(data_);
    }
  }

  ~session() { dispose(); }

  session(session&& other) { *this = std::move(other); }

  session& operator=(session&& other) {
    dispose();
    data_ = other.data_;
    other.data_ = nullptr;
    return *this;
  }

  prepared prepare(const char* query) const { return prepared{data_, query}; }

  template <typename... params_t>
  future execute(const char* query, params_t... params) {
    statement s{query, sizeof...(params)};
    bind_parameters(0, s, std::forward<params_t>(params)...);
    return execute(s);
  }

  future execute(const statement& s) {
    auto f = future(cass_session_execute(data_, s.data()), true);
    f.wait();
    return f;
  }

  future execute_async(const statement& s) {
    auto f = future(cass_session_execute(data_, s.data()), true);
    return f;
  }

  future execute_async(const batch& b) {
    auto f = future(cass_session_execute_batch(data_, b.data()), true);
    return f;
  }

  void execute(const batch& b) {
    future(cass_session_execute_batch(data_, b.data())).wait();
  }

 private:
  void bind_parameters(size_t, statement&) {}

  template <typename first_param_t_, typename... params_t>
  void bind_parameters(size_t param_pos,
                       statement& s,
                       first_param_t_&& p,
                       params_t... params) {
    using first_param_t = typename std::remove_cv<
        typename std::remove_reference<first_param_t_>::type>::type;
    if constexpr (std::is_convertible<first_param_t, const char*>::value) {
      s.bind_string(param_pos, static_cast<const char*>(p));
    } else if constexpr (std::is_same<first_param_t, std::string>::value) {
      s.bind_string(param_pos, p);
    } else if constexpr (std::is_same<first_param_t, int8_t>::value) {
      s.bind_int8(param_pos, p);
    } else if constexpr (std::is_same<first_param_t, int16_t>::value) {
      s.bind_int16(param_pos, p);
    } else if constexpr (std::is_same<first_param_t, int32_t>::value) {
      s.bind_int32(param_pos, p);
    } else if constexpr (std::is_same<first_param_t, uint32_t>::value) {
      s.bind_uint32(param_pos, p);
    } else if constexpr (std::is_same<first_param_t, int64_t>::value) {
      s.bind_int64(param_pos, p);
    } else if constexpr (std::is_same<first_param_t, int64_t>::value) {
      s.bind_float(param_pos, p);
    } else if constexpr (std::is_same<first_param_t, float>::value) {
      s.bind_float(param_pos, p);
    } else if constexpr (std::is_same<first_param_t, double>::value) {
      s.bind_double(param_pos, p);
    } else if constexpr (std::is_same<first_param_t,
                                      std::vector<cass_byte_t>>::value) {
      s.bind_bytes(param_pos, p);
    }

    if constexpr (sizeof...(params_t) > 0) {
      bind_parameters(param_pos + 1, s, params...);
    }
  }

  session(CassCluster* cluster) {
    data_ = cass_session_new();

    future(cass_session_connect(data_, cluster)).wait();
  }

  session(CassCluster* cluster, const char* keyspace_name) {
    data_ = cass_session_new();

    future(cass_session_connect_keyspace(data_, cluster, keyspace_name)).wait();
  }

  session(const session&) = delete;
  session& operator=(const session&) = delete;

  mutable CassSession* data_{nullptr};
};

class cluster {
 public:
  /**
   * Default constructor
   *
   */
  cluster() { data_ = cass_cluster_new(); }

  /**
   * Prevent copy
   */
  cluster(const cluster&) = delete;
  cluster& operator=(const cluster&) = delete;

  /**
   * @brief Move constructor
   *
   * @param other Object to move from
   */
  cluster(cluster&& other) { *this = std::move(other); }

  /**
   * @brief Move operator
   *
   * @param other Object to move from
   *
   * @return Reference to self
   */
  cluster& operator=(cluster&& other) {
    dispose();
    data_ = other.data_;
    other.data_ = nullptr;
    return *this;
  }

  /**
   * @brief Creates cluster object and adds contact points
   *
   * @param contact_points String with comma separated list of contact points
   */
  cluster(const char* contact_points) : cluster() {
    set_contact_points(contact_points);
    set_protocol_version(4);
  }

  cluster(const std::string& contact_points)
      : cluster(contact_points.c_str()) {}

  ~cluster() { dispose(); }

  void dispose() {
    if (data_ != nullptr) {
      cass_cluster_free(data_);
    }
  }

  cluster& set_contact_points(const char* contact_points) {
    error::check_and_throw(
        cass_cluster_set_contact_points(data_, contact_points));
    return *this;
  }

  cluster& set_port(int port) {
    error::check_and_throw(cass_cluster_set_port(data_, port));
    return *this;
  }

  template <typename Rep, typename Period>
  cluster& set_request_timeout(
      const std::chrono::duration<Rep, Period> timeout) {
    const auto timeout_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count();
    if (timeout_ms > std::numeric_limits<unsigned>::max()) {
      throw std::runtime_error{
          "cassandrapp::cluster::set_timeout() Invalid timeout value"};
    }
    cass_cluster_set_request_timeout(data_, timeout_ms);

    return *this;
  }

  /**
   * @brief Set the protocol version
   *
   * @param version
   * @return cluster&
   */
  cluster& set_protocol_version(int version) {
    error::check_and_throw(cass_cluster_set_protocol_version(data_, version));
    return *this;
  }

  session connect() { return session(data_); }

  session connect(const char* keyspace_name) {
    return session(data_, keyspace_name);
  }

  /**
   * Returns pointer to underlying object
   */
  CassCluster* data() const { return data_; }

 private:
  mutable CassCluster* data_{nullptr};
};

class auto_batch {
 public:
  auto_batch(session& cass_session, size_t max_size)
      : cass_session_{cass_session}, batch_{}, max_size_{max_size} {}

  void add_statement(const statement& s) {
    batch_.add_statement(s);
    if (batch_.size() >= max_size_) {
      cass_session_.execute(batch_);
      batch_ = batch{};
    }
  }

  session& cass_session() { return cass_session_; }

  ~auto_batch() {
    if (batch_.size() > 0) {
      cass_session_.execute(batch_);
    }
  }

 private:
  session& cass_session_;
  batch batch_;
  const size_t max_size_;
};

}  // namespace cassandrapp
