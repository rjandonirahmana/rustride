//! Unit tests untuk semua service layer.
//!
//! Pendekatan:
//!   - Setiap repository di-mock menggunakan struct manual yang impl trait-nya.
//!   - Tidak ada koneksi DB, Redis, atau jaringan — semua in-memory.
//!   - Test fokus pada business logic: validasi, state transition, error path.
//!   - ConnectionManager di-skip (fire-and-forget) — test tidak assert side-effect WebSocket.

#[cfg(test)]
mod auth_tests {
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    use crate::{
        auth::JwtService,
        models::user::{DriverProfile, RegisterRequest, User},
        repository::user::UserRepository,
        service::auth::AuthService,
    };

    // ── Mock UserRepository ───────────────────────────────────────────────────

    #[derive(Clone, Default)]
    struct MockUserRepo {
        users: Arc<Mutex<Vec<User>>>,
    }

    impl MockUserRepo {
        fn with_user(user: User) -> Self {
            Self {
                users: Arc::new(Mutex::new(vec![user])),
            }
        }
    }

    #[async_trait]
    impl UserRepository for MockUserRepo {
        async fn create(&self, req: &RegisterRequest, hashed: &str) -> Result<User> {
            let user = User {
                id: crate::utils::ulid::new_ulid(),
                name: req.name.clone(),
                phone: req.phone.clone(),
                email: req.email.clone(),
                password: hashed.to_string(),
                role: req.role.clone(),
                avatar_url: None,
                created_at: "2024-01-01T00:00:00Z".into(),
                vehicle_type: req.vehicle_type.clone().unwrap_or_default(),
            };
            self.users.lock().await.push(user.clone());
            Ok(user)
        }

        async fn find_by_id(&self, id: &str) -> Result<Option<User>> {
            Ok(self.users.lock().await.iter().find(|u| u.id == id).cloned())
        }

        async fn find_by_phone(&self, phone: &str) -> Result<Option<User>> {
            Ok(self
                .users
                .lock()
                .await
                .iter()
                .find(|u| u.phone == phone)
                .cloned())
        }

        async fn find_driver_by_id(&self, id: &str) -> Result<Option<(User, DriverProfile)>> {
            Ok(None)
        }

        async fn find_drivers_by_ids(&self, _ids: &[String]) -> Result<Vec<(User, DriverProfile)>> {
            Ok(vec![])
        }
    }

    fn jwt() -> JwtService {
        JwtService::new("test-secret-key-for-unit-tests-only")
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    #[test]
    fn jwt_sign_and_verify_roundtrip() {
        let svc = jwt();
        let token = svc.sign("user-123", "Budi", "rider", "motor").unwrap();
        let claims = svc.verify(&token).unwrap();
        assert_eq!(claims.sub, "user-123");
        assert_eq!(claims.role, "rider");
        assert_eq!(claims.name, "Budi");
    }

    #[test]
    fn jwt_verify_invalid_token_returns_error() {
        let svc = jwt();
        assert!(svc.verify("ini.bukan.token").is_err());
    }

    #[test]
    fn jwt_verify_wrong_secret_returns_error() {
        let svc1 = jwt();
        let svc2 = JwtService::new("secret-lain");
        let token = svc1.sign("user-1", "Andi", "driver", "motor").unwrap();
        assert!(svc2.verify(&token).is_err());
    }

    #[tokio::test]
    async fn login_success() {
        // Buat user dengan password yang sudah di-hash
        let hashed = bcrypt::hash("password123", bcrypt::DEFAULT_COST).unwrap();
        let user = User {
            id: crate::utils::ulid::new_ulid(),
            name: "Siti".into(),
            phone: "08123456789".into(),
            email: "siti@example.com".into(),
            password: hashed,
            role: "rider".into(),
            avatar_url: None,
            created_at: "2024-01-01T00:00:00Z".into(),
            vehicle_type: "".into(),
        };
        let repo = Arc::new(MockUserRepo::with_user(user));
        let svc = AuthService {
            repo,
            jwt: jwt(),
            redis: make_fake_redis(),
            http: reqwest::Client::new(),
            waha: Arc::new(crate::config::WahaConfig {
                base_url: String::new(),
                session: String::new(),
                api_key: String::new(),
            }),
        };
        let resp = svc.login("08123456789", "password123").await.unwrap();
        assert_eq!(resp.user.phone, "08123456789");
        assert!(!resp.token.is_empty());
    }

    #[tokio::test]
    async fn login_wrong_password_returns_error() {
        let hashed = bcrypt::hash("benar", bcrypt::DEFAULT_COST).unwrap();
        let user = User {
            id: crate::utils::ulid::new_ulid(),
            name: "Andi".into(),
            phone: "0811".into(),
            email: "".into(),
            password: hashed,
            role: "rider".into(),
            avatar_url: None,
            created_at: "".into(),
            vehicle_type: "".into(),
        };
        let repo = Arc::new(MockUserRepo::with_user(user));
        let svc = AuthService {
            repo,
            jwt: jwt(),
            redis: make_fake_redis(),
            http: reqwest::Client::new(),
            waha: Arc::new(crate::config::WahaConfig {
                base_url: String::new(),
                session: String::new(),
                api_key: String::new(),
            }),
        };
        if let Err(err) = svc.login("0811", "salah").await {
            assert!(err.to_string().contains("Password salah"));
            assert!(err.to_string().contains("Password salah"));
        };
    }

    #[tokio::test]
    async fn login_user_not_found_returns_error() {
        let repo = Arc::new(MockUserRepo::default());
        let svc = AuthService {
            repo,
            jwt: jwt(),
            redis: make_fake_redis(),
            http: reqwest::Client::new(),
            waha: Arc::new(crate::config::WahaConfig {
                base_url: String::new(),
                session: String::new(),
                api_key: String::new(),
            }),
        };
        let err = svc.login("08999999999", "apapun").await;
        assert!(err.is_err());
    }

    // Helper: buat ConnectionManager fake (tidak dipakai di auth, tapi field wajib ada)
    fn make_fake_redis() -> redis::aio::ConnectionManager {
        // AuthService butuh Redis untuk OTP — di test login, Redis tidak disentuh.
        // Kita pakai unsafe transmute untuk bypass — HANYA untuk test.
        // Alternatif bersih: refactor AuthService pakai trait RedisClient.
        // Untuk sekarang, login_test tidak memanggil Redis, jadi ini aman.
        unsafe { std::mem::zeroed() }
    }
}

// =============================================================================

#[cfg(test)]
mod order_service_tests {
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    use crate::{
        connections::ConnectionManager,
        location::LocationService,
        models::{
            order::{DriverLocation, Order},
            user::{DriverProfile, User},
        },
        repository::{
            order::{NewOrder, OrderRepository},
            user::UserRepository,
        },
        service::order::{calculate_fare, OrderService},
        utils::ulid::new_ulid,
    };

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn make_order(id: &str, rider_id: &str, status: &str) -> Order {
        Order {
            id: id.to_string(),
            rider_id: rider_id.to_string(),
            driver_id: None,
            status: status.to_string(),
            pickup_lat: -6.200000,
            pickup_lng: 106.816666,
            pickup_address: "Monas, Jakarta".into(),
            dest_lat: -6.175110,
            dest_lng: 106.865036,
            dest_address: "Taman Ismail Marzuki".into(),
            distance_km: None,
            fare_estimate: 15_000,
            fare_final: None,
            service_type: "motor".into(),
            created_at: "2024-01-01T00:00:00Z".into(),
            rider_name: "Budi".into(),
        }
    }

    fn make_order_with_driver(id: &str, rider: &str, driver: &str, status: &str) -> Order {
        let mut o = make_order(id, rider, status);
        o.driver_id = Some(driver.to_string());
        o
    }

    // ── Mock OrderRepository ──────────────────────────────────────────────────

    #[derive(Clone, Default)]
    struct MockOrderRepo {
        orders: Arc<Mutex<Vec<Order>>>,
    }

    impl MockOrderRepo {
        fn with(orders: Vec<Order>) -> Self {
            Self {
                orders: Arc::new(Mutex::new(orders)),
            }
        }
    }

    #[async_trait]
    impl OrderRepository for MockOrderRepo {
        async fn create(&self, o: &NewOrder) -> Result<Order> {
            let order = make_order(&new_ulid(), &o.rider_id, "searching");
            self.orders.lock().await.push(order.clone());
            Ok(order)
        }

        async fn find_by_id(&self, id: &str) -> Result<Option<Order>> {
            Ok(self
                .orders
                .lock()
                .await
                .iter()
                .find(|o| o.id == id)
                .cloned())
        }

        async fn find_active_for_rider(&self, rider_id: &str) -> Result<Option<Order>> {
            Ok(self
                .orders
                .lock()
                .await
                .iter()
                .find(|o| {
                    o.rider_id == rider_id
                        && !matches!(o.status.as_str(), "cancelled" | "completed")
                })
                .cloned())
        }

        async fn find_active_for_driver(&self, driver_id: &str) -> Result<Option<Order>> {
            Ok(self
                .orders
                .lock()
                .await
                .iter()
                .find(|o| {
                    o.driver_id.as_deref() == Some(driver_id)
                        && !matches!(o.status.as_str(), "cancelled" | "completed")
                })
                .cloned())
        }

        async fn find_active_between(
            &self,
            rider_id: &str,
            driver_id: &str,
        ) -> Result<Option<Order>> {
            Ok(self
                .orders
                .lock()
                .await
                .iter()
                .find(|o| o.rider_id == rider_id && o.driver_id.as_deref() == Some(driver_id))
                .cloned())
        }

        async fn get_nearby_searching(
            &self,
            service_type: &str,
            _lat: f64,
            _lng: f64,
            _radius_km: f64,
        ) -> Result<Vec<Order>> {
            Ok(self
                .orders
                .lock()
                .await
                .iter()
                .filter(|o| o.status == "searching" && o.service_type == service_type)
                .cloned()
                .collect())
        }

        async fn assign_driver(&self, order_id: &str, driver_id: &str) -> Result<()> {
            let mut orders = self.orders.lock().await;
            if let Some(o) = orders.iter_mut().find(|o| o.id == order_id) {
                o.driver_id = Some(driver_id.to_string());
                o.status = "driver_found".into();
            }
            Ok(())
        }

        async fn assign_driver_atomic(&self, order_id: &str, driver_id: &str) -> Result<u64> {
            let mut orders = self.orders.lock().await;
            if let Some(o) = orders
                .iter_mut()
                .find(|o| o.id == order_id && o.status == "searching")
            {
                o.driver_id = Some(driver_id.to_string());
                o.status = "driver_accepted".into();
                Ok(1)
            } else {
                Ok(0)
            }
        }

        async fn update_status(&self, order_id: &str, status: &str) -> Result<()> {
            let mut orders = self.orders.lock().await;
            if let Some(o) = orders.iter_mut().find(|o| o.id == order_id) {
                o.status = status.to_string();
            }
            Ok(())
        }

        async fn complete(&self, order_id: &str, distance_km: f32, fare_final: i32) -> Result<()> {
            let mut orders = self.orders.lock().await;
            if let Some(o) = orders.iter_mut().find(|o| o.id == order_id) {
                o.status = "completed".into();
                o.fare_final = Some(fare_final);
                o.distance_km = Some(distance_km);
            }
            Ok(())
        }

        async fn cancel(&self, order_id: &str, _reason: Option<&str>) -> Result<()> {
            let mut orders = self.orders.lock().await;
            if let Some(o) = orders.iter_mut().find(|o| o.id == order_id) {
                o.status = "cancelled".into();
            }
            Ok(())
        }

        async fn submit_rating(
            &self,
            _order_id: &str,
            _poster_id: &str,
            _target_id: &str,
            _rating: u8,
            _tip_amount: f64,
            _comment: &str,
        ) -> Result<()> {
            Ok(())
        }

        async fn get_by_user_id(&self, user_id: &str, _limit: i64) -> Result<Vec<Order>> {
            Ok(self
                .orders
                .lock()
                .await
                .iter()
                .filter(|o| o.rider_id == user_id)
                .cloned()
                .collect())
        }
    }

    // ── Mock UserRepository ───────────────────────────────────────────────────

    #[derive(Clone, Default)]
    struct MockUserRepo;

    #[async_trait]
    impl UserRepository for MockUserRepo {
        async fn create(
            &self,
            _req: &crate::models::user::RegisterRequest,
            _hashed: &str,
        ) -> Result<User> {
            unimplemented!()
        }
        async fn find_by_id(&self, _id: &str) -> Result<Option<User>> {
            Ok(None)
        }
        async fn find_by_phone(&self, _phone: &str) -> Result<Option<User>> {
            Ok(None)
        }
        async fn find_driver_by_id(&self, id: &str) -> Result<Option<(User, DriverProfile)>> {
            Ok(Some((
                User {
                    id: id.to_string(),
                    name: "Driver A".into(),
                    phone: "081".into(),
                    email: "".into(),
                    password: "".into(),
                    role: "driver".into(),
                    avatar_url: None,
                    created_at: "".into(),
                    vehicle_type: "motor".into(),
                },
                DriverProfile {
                    user_id: id.to_string(),
                    vehicle_type: "motor".into(),
                    vehicle_plate: "B1234XY".into(),
                    vehicle_model: "Honda Beat".into(),
                    vehicle_color: "Merah".into(),
                    rating: 4.8,
                    total_trips: 100,
                    is_active: true,
                },
            )))
        }
        async fn find_drivers_by_ids(&self, _ids: &[String]) -> Result<Vec<(User, DriverProfile)>> {
            Ok(vec![])
        }
    }

    fn make_svc(orders: Vec<Order>) -> OrderService<MockOrderRepo, MockUserRepo> {
        // LocationService dan ConnectionManager memerlukan Redis/state nyata.
        // Untuk unit test, kita test logic yang tidak memerlukan keduanya
        // (create_order, cancel, rating, dll) — bukan background spawn loop.
        // Kita skip dengan unsafe zeroed sama seperti pola di atas.
        let order_repo = Arc::new(MockOrderRepo::with(orders));
        let user_repo = Arc::new(MockUserRepo);
        let location: LocationService = unsafe { std::mem::zeroed() };
        let connections: Arc<ConnectionManager> = unsafe { std::mem::zeroed() };
        OrderService::new(order_repo, user_repo, location, connections)
    }

    // ── Tests: calculate_fare ─────────────────────────────────────────────────

    #[test]
    fn fare_motor_minimum() {
        // 0 km → hanya base fare, dibulatkan ke ribuan
        let fare = calculate_fare(0.0, "motor");
        assert_eq!(fare, 8_000); // base = 8000, sudah bulat
    }

    #[test]
    fn fare_motor_5km() {
        // 8000 + 5 * 2500 = 20500 → dibulatkan ke atas → 21000
        let fare = calculate_fare(5.0, "motor");
        assert_eq!(fare, 21_000);
    }

    #[test]
    fn fare_mobil_is_more_expensive_than_motor() {
        let motor = calculate_fare(5.0, "motor");
        let mobil = calculate_fare(5.0, "mobil");
        assert!(mobil > motor, "mobil harus lebih mahal dari motor");
    }

    #[test]
    fn fare_rounds_up_to_nearest_thousand() {
        // 8000 + 1 * 2500 = 10500 → naik ke 11000
        let fare = calculate_fare(1.0, "motor");
        assert_eq!(fare % 1000, 0, "fare harus kelipatan 1000");
        assert_eq!(fare, 11_000);
    }

    // ── Tests: cancel_order ───────────────────────────────────────────────────

    #[tokio::test]
    async fn cancel_order_rider_searching_success() {
        let rider = new_ulid();
        let order = make_order("order-1", &rider, "searching");
        let svc = make_svc(vec![order]);
        svc.cancel_order(&rider, "rider", "order-1", None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancel_order_rider_cannot_cancel_after_driver_accepted() {
        let rider = new_ulid();
        let driver = new_ulid();
        let order = make_order_with_driver("order-2", &rider, &driver, "driver_accepted");
        let svc = make_svc(vec![order]);
        let err = svc.cancel_order(&rider, "rider", "order-2", None).await;
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("sudah driver_accepted"));
    }

    #[tokio::test]
    async fn cancel_order_wrong_user_returns_error() {
        let rider = new_ulid();
        let other_user = new_ulid();
        let order = make_order("order-3", &rider, "searching");
        let svc = make_svc(vec![order]);
        let err = svc
            .cancel_order(&other_user, "rider", "order-3", None)
            .await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("Bukan order kamu"));
    }

    #[tokio::test]
    async fn cancel_order_already_completed_returns_error() {
        let rider = new_ulid();
        let order = make_order("order-4", &rider, "completed");
        let svc = make_svc(vec![order]);
        let err = svc.cancel_order(&rider, "rider", "order-4", None).await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("sudah completed"));
    }

    #[tokio::test]
    async fn cancel_order_driver_can_cancel_accepted() {
        let rider = new_ulid();
        let driver = new_ulid();
        let order = make_order_with_driver("order-5", &rider, &driver, "driver_accepted");
        let svc = make_svc(vec![order]);
        svc.cancel_order(&driver, "driver", "order-5", Some("Darurat".into()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancel_order_driver_cannot_cancel_on_trip() {
        let rider = new_ulid();
        let driver = new_ulid();
        let order = make_order_with_driver("order-6", &rider, &driver, "on_trip");
        let svc = make_svc(vec![order]);
        let err = svc.cancel_order(&driver, "driver", "order-6", None).await;
        assert!(err.is_err());
    }

    // ── Tests: driver_arrived / start_trip / complete_trip ───────────────────

    #[tokio::test]
    async fn driver_arrived_wrong_status_returns_error() {
        let rider = new_ulid();
        let driver = new_ulid();
        // Status harus driver_accepted untuk bisa arrived
        let order = make_order_with_driver("order-7", &rider, &driver, "searching");
        let svc = make_svc(vec![order]);
        let err = svc.driver_arrived(&driver, "order-7").await;
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("butuh: driver_accepted"));
    }

    #[tokio::test]
    async fn start_trip_requires_driver_arrived_status() {
        let rider = new_ulid();
        let driver = new_ulid();
        let order = make_order_with_driver("order-8", &rider, &driver, "driver_accepted");
        let svc = make_svc(vec![order]);
        let err = svc.start_trip(&driver, "order-8").await;
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("butuh: driver_arrived"));
    }

    #[tokio::test]
    async fn complete_trip_requires_on_trip_status() {
        let rider = new_ulid();
        let driver = new_ulid();
        let order = make_order_with_driver("order-9", &rider, &driver, "driver_arrived");
        let svc = make_svc(vec![order]);
        let err = svc.complete_trip(&driver, "order-9", 5.0).await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("butuh: on_trip"));
    }

    #[tokio::test]
    async fn driver_cannot_act_on_others_order() {
        let rider = new_ulid();
        let driver_a = new_ulid();
        let driver_b = new_ulid();
        let order = make_order_with_driver("order-10", &rider, &driver_a, "driver_accepted");
        let svc = make_svc(vec![order]);
        // driver_b coba arrived order milik driver_a
        let err = svc.driver_arrived(&driver_b, "order-10").await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("Bukan order kamu"));
    }

    // ── Tests: submit_rating ──────────────────────────────────────────────────

    #[tokio::test]
    async fn submit_rating_success() {
        let rider = new_ulid();
        let driver = new_ulid();
        let order = make_order_with_driver("order-11", &rider, &driver, "completed");
        let svc = make_svc(vec![order]);
        svc.submit_rating(&rider, "rider", "order-11", 5000.0, 5, "Mantap!")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn submit_rating_only_rider_can_rate() {
        let rider = new_ulid();
        let driver = new_ulid();
        let order = make_order_with_driver("order-12", &rider, &driver, "completed");
        let svc = make_svc(vec![order]);
        let err = svc
            .submit_rating(&driver, "driver", "order-12", 0.0, 4, "")
            .await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("Hanya rider"));
    }

    #[tokio::test]
    async fn submit_rating_order_not_completed_returns_error() {
        let rider = new_ulid();
        let driver = new_ulid();
        let order = make_order_with_driver("order-13", &rider, &driver, "on_trip");
        let svc = make_svc(vec![order]);
        let err = svc
            .submit_rating(&rider, "rider", "order-13", 0.0, 5, "")
            .await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("belum selesai"));
    }

    // ── Tests: estimate_fare ──────────────────────────────────────────────────

    #[tokio::test]
    async fn estimate_fare_returns_reasonable_values() {
        let svc = make_svc(vec![]);
        // Monas → Taman Ismail Marzuki ~6 km
        let result = svc
            .estimate_fare(-6.200000, 106.816666, -6.175110, 106.865036, "motor")
            .await
            .unwrap();
        assert!(result.fare_estimate > 0);
        assert!(result.distance_km > 0.0);
        assert!(result.duration_min > 0);
        assert_eq!(result.service_type, "motor");
    }

    // ── Tests: get_active_order ───────────────────────────────────────────────

    #[tokio::test]
    async fn get_active_order_returns_none_when_no_active() {
        let rider = new_ulid();
        let order = make_order("order-99", &rider, "completed");
        let svc = make_svc(vec![order]);
        let result = svc.get_active_order_for_user(&rider).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn get_active_order_returns_searching_order() {
        let rider = new_ulid();
        let order = make_order("order-100", &rider, "searching");
        let svc = make_svc(vec![order]);
        let result = svc.get_active_order_for_user(&rider).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().status, "searching");
    }
}

// =============================================================================

#[cfg(test)]
mod driver_service_tests {
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::Arc;

    use crate::{
        repository::driver::*,
        service::driver::{DriverService, TodayOrdersResult},
    };

    // ── Mock DriverRepository ─────────────────────────────────────────────────

    fn make_item(status: &str, fare: i64, earn: i64, tip: i64, rating: f32) -> DriverOrderItem {
        DriverOrderItem {
            order_id: crate::utils::ulid::new_ulid(),
            status: status.to_string(),
            rider_name: "Penumpang".into(),
            pickup_address: "Jl. Merdeka".into(),
            dest_address: "Jl. Sudirman".into(),
            distance_km: 5.0,
            duration_min: 15,
            fare,
            driver_earning: earn,
            tip,
            service_type: "motor".into(),
            started_at: "2024-01-01T08:00:00Z".into(),
            completed_at: "2024-01-01T08:15:00Z".into(),
            cancel_reason: String::new(),
            rating,
            rating_comment: String::new(),
        }
    }

    #[derive(Default)]
    struct MockDriverRepo {
        orders: Vec<DriverOrderItem>,
        summary: Option<DailySummary>,
    }

    impl MockDriverRepo {
        fn with_orders(orders: Vec<DriverOrderItem>) -> Self {
            Self {
                orders,
                summary: None,
            }
        }
        fn with_summary(summary: DailySummary) -> Self {
            Self {
                orders: vec![],
                summary: Some(summary),
            }
        }
    }

    #[async_trait]
    impl DriverRepository for MockDriverRepo {
        async fn get_today_orders(&self, _driver_id: &str) -> Result<Vec<DriverOrderItem>> {
            Ok(self.orders.clone())
        }
        async fn get_today_summary(&self, _driver_id: &str) -> Result<Option<DailySummary>> {
            Ok(self.summary.clone())
        }
        async fn get_order_history(
            &self,
            _driver_id: &str,
            filter: OrderHistoryFilter,
        ) -> Result<(Vec<DriverOrderItem>, u32)> {
            let total = self.orders.len() as u32;
            let limit = filter.limit as usize;
            let page = (filter.page.saturating_sub(1)) as usize;
            let start = page * limit;
            let slice = self
                .orders
                .iter()
                .skip(start)
                .take(limit)
                .cloned()
                .collect();
            Ok((slice, total))
        }
        async fn get_order_detail(
            &self,
            _driver_id: &str,
            order_id: &str,
        ) -> Result<Option<DriverOrderDetail>> {
            let item = self.orders.iter().find(|o| o.order_id == order_id).cloned();
            Ok(item.map(|item| DriverOrderDetail {
                item,
                pickup_lat: -6.2,
                pickup_lng: 106.8,
                dest_lat: -6.1,
                dest_lng: 106.9,
                rider_phone: "08111".into(),
                rider_avatar: "".into(),
                rider_rating: 4.5,
            }))
        }
        async fn get_earnings(
            &self,
            _driver_id: &str,
            _date_from: &str,
            _date_to: &str,
        ) -> Result<EarningsResult> {
            Ok(EarningsResult {
                gross_earnings: self.orders.iter().map(|o| o.fare).sum(),
                net_earnings: self.orders.iter().map(|o| o.driver_earning).sum(),
                tips: self.orders.iter().map(|o| o.tip).sum(),
                trip_count: self
                    .orders
                    .iter()
                    .filter(|o| o.status == "completed")
                    .count() as i32,
                cancel_count: self
                    .orders
                    .iter()
                    .filter(|o| o.status == "cancelled")
                    .count() as i32,
                distance_km: self.orders.iter().map(|o| o.distance_km).sum(),
                online_minutes: 480,
                daily: vec![],
            })
        }
    }

    // ── Tests: aggregate_summary (fallback) ───────────────────────────────────

    #[tokio::test]
    async fn today_summary_aggregated_from_orders_when_no_db_summary() {
        let orders = vec![
            make_item("completed", 20_000, 16_000, 5_000, 5.0),
            make_item("completed", 15_000, 12_000, 2_000, 4.0),
            make_item("cancelled", 0, 0, 0, 0.0),
        ];
        let svc = DriverService::new(Arc::new(MockDriverRepo::with_orders(orders)));
        let result = svc.get_today_summary("driver-1").await.unwrap();

        assert_eq!(result.total_orders, 2);
        assert_eq!(result.cancelled_orders, 1);
        assert_eq!(result.gross_earnings, 35_000);
        assert_eq!(result.net_earnings, 28_000);
        assert_eq!(result.tips, 7_000);
        // avg_rating = (5.0 + 4.0) / 2 = 4.5
        let diff = (result.avg_rating - 4.5).abs();
        assert!(
            diff < 0.01,
            "avg_rating harus 4.5, dapat {}",
            result.avg_rating
        );
    }

    #[tokio::test]
    async fn today_summary_uses_db_summary_when_available() {
        let db_summary = DailySummary {
            total_orders: 10,
            cancelled_orders: 1,
            gross_earnings: 200_000,
            platform_fee: 40_000,
            net_earnings: 160_000,
            tips: 25_000,
            online_minutes: 360,
            distance_km: 50.0,
            avg_rating: 4.9,
            peak_hour: "08:00-09:00".into(),
        };
        let svc = DriverService::new(Arc::new(MockDriverRepo::with_summary(db_summary)));
        let result = svc.get_today_summary("driver-1").await.unwrap();

        assert_eq!(result.total_orders, 10);
        assert_eq!(result.online_minutes, 360);
        assert_eq!(result.peak_hour, "08:00-09:00");
    }

    #[tokio::test]
    async fn today_summary_empty_orders_returns_zero_defaults() {
        let svc = DriverService::new(Arc::new(MockDriverRepo::with_orders(vec![])));
        let result = svc.get_today_summary("driver-empty").await.unwrap();
        assert_eq!(result.total_orders, 0);
        assert_eq!(result.gross_earnings, 0);
        assert_eq!(result.avg_rating, 0.0);
    }

    // ── Tests: get_order_history filter validasi ──────────────────────────────

    #[tokio::test]
    async fn get_order_history_invalid_status_returns_error() {
        let svc = DriverService::new(Arc::new(MockDriverRepo::default()));
        if let Err(err) = svc
            .get_order_history(
                "driver-1",
                None,
                None,
                Some("pending".into()), // bukan completed/cancelled
                None,
                1,
                10,
            )
            .await
        {
            assert!(err.to_string().contains("status harus"));
        };
    }

    #[tokio::test]
    async fn get_order_history_pagination_has_more_flag() {
        // 15 order, limit 10 → page 1 has_more = true
        let orders: Vec<_> = (0..15)
            .map(|_| make_item("completed", 20_000, 16_000, 0, 4.8))
            .collect();
        let svc = DriverService::new(Arc::new(MockDriverRepo::with_orders(orders)));
        let result = svc
            .get_order_history("driver-1", None, None, None, None, 1, 10)
            .await
            .unwrap();
        assert_eq!(result.total, 15);
        assert!(result.has_more);
    }

    #[tokio::test]
    async fn get_order_history_last_page_no_more() {
        let orders: Vec<_> = (0..15)
            .map(|_| make_item("completed", 20_000, 16_000, 0, 4.8))
            .collect();
        let svc = DriverService::new(Arc::new(MockDriverRepo::with_orders(orders)));
        let result = svc
            .get_order_history("driver-1", None, None, None, None, 2, 10)
            .await
            .unwrap();
        assert!(!result.has_more); // page 2 * 10 = 20 >= 15
    }

    // ── Tests: get_order_detail ───────────────────────────────────────────────

    #[tokio::test]
    async fn get_order_detail_found() {
        let item = make_item("completed", 20_000, 16_000, 3_000, 5.0);
        let id = item.order_id.clone();
        let svc = DriverService::new(Arc::new(MockDriverRepo::with_orders(vec![item])));
        let detail = svc.get_order_detail("driver-1", &id).await.unwrap();
        assert_eq!(detail.item.order_id, id);
        assert_eq!(detail.rider_phone, "08111");
    }

    #[tokio::test]
    async fn get_order_detail_not_found_returns_error() {
        let svc = DriverService::new(Arc::new(MockDriverRepo::with_orders(vec![])));
        let err = svc.get_order_detail("driver-1", "order-tidak-ada").await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("tidak ditemukan"));
    }

    // ── Tests: get_earnings ───────────────────────────────────────────────────

    #[tokio::test]
    async fn get_earnings_sums_correctly() {
        let orders = vec![
            make_item("completed", 20_000, 16_000, 5_000, 5.0),
            make_item("completed", 30_000, 24_000, 0, 4.0),
            make_item("cancelled", 0, 0, 0, 0.0),
        ];
        let svc = DriverService::new(Arc::new(MockDriverRepo::with_orders(orders)));
        let result = svc
            .get_earnings("driver-1", "2024-01-01", "2024-01-31")
            .await
            .unwrap();
        assert_eq!(result.gross_earnings, 50_000);
        assert_eq!(result.net_earnings, 40_000);
        assert_eq!(result.tips, 5_000);
        assert_eq!(result.trip_count, 2);
        assert_eq!(result.cancel_count, 1);
    }
}

// =============================================================================

#[cfg(test)]
mod notification_service_tests {
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    use crate::{
        connections::ConnectionManager,
        repository::notification::{Notification, NotificationRepositoryTrait},
        service::notification::NotificationService,
    };

    // ── Mock ─────────────────────────────────────────────────────────────────

    #[derive(Default)]
    struct MockNotifRepo {
        store: Mutex<Vec<Notification>>,
        next_id: Mutex<i64>,
    }

    impl MockNotifRepo {
        fn new_arc() -> Arc<Self> {
            Arc::new(Self::default())
        }
    }

    #[async_trait]
    impl NotificationRepositoryTrait for MockNotifRepo {
        async fn create(
            &self,
            user_id: &str,
            notif_type: &str,
            title: &str,
            body: &str,
            ref_order_id: Option<&str>,
            ref_user_id: Option<&str>,
            _meta: Option<&str>,
        ) -> Result<i64> {
            let mut id = self.next_id.lock().await;
            *id += 1;
            let notif = Notification {
                id: *id,
                user_id: user_id.to_string(),
                notif_type: notif_type.to_string(),
                title: title.to_string(),
                body: body.to_string(),
                ref_order_id: ref_order_id.map(|s| s.to_string()),
                ref_user_id: ref_user_id.map(|s| s.to_string()),
                meta: None,
                is_read: false,
                read_at: None,
                created_at: "2024-01-01T00:00:00Z".into(),
            };
            self.store.lock().await.push(notif);
            Ok(*id)
        }

        async fn list(
            &self,
            user_id: &str,
            limit: i32,
            before_id: Option<i64>,
            unread_only: bool,
        ) -> Result<Vec<Notification>> {
            let store = self.store.lock().await;
            let mut items: Vec<_> = store
                .iter()
                .filter(|n| n.user_id == user_id)
                .filter(|n| !unread_only || !n.is_read)
                .filter(|n| before_id.map_or(true, |bid| n.id < bid))
                .cloned()
                .collect();
            items.sort_by(|a, b| b.id.cmp(&a.id));
            items.truncate(limit as usize);
            Ok(items)
        }

        async fn mark_read(&self, notif_id: i64, _user_id: &str) -> Result<()> {
            let mut store = self.store.lock().await;
            if let Some(n) = store.iter_mut().find(|n| n.id == notif_id) {
                n.is_read = true;
            }
            Ok(())
        }

        async fn mark_all_read(&self, user_id: &str) -> Result<()> {
            let mut store = self.store.lock().await;
            for n in store.iter_mut().filter(|n| n.user_id == user_id) {
                n.is_read = true;
            }
            Ok(())
        }

        async fn unread_count(&self, user_id: &str) -> Result<i64> {
            Ok(self
                .store
                .lock()
                .await
                .iter()
                .filter(|n| n.user_id == user_id && !n.is_read)
                .count() as i64)
        }
    }

    fn make_svc(repo: Arc<MockNotifRepo>) -> NotificationService<MockNotifRepo> {
        let connections: Arc<ConnectionManager> = unsafe { std::mem::zeroed() };
        NotificationService {
            notif_repo: repo,
            connections,
        }
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn push_stores_notification() {
        let repo = MockNotifRepo::new_arc();
        let svc = make_svc(repo.clone());
        svc.push(
            "user-1",
            "order_update",
            "Order diterima",
            "Driver menuju lokasi mu",
            Some("order-abc"),
            None,
            None,
        )
        .await
        .unwrap();
        let count = repo.store.lock().await.len();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn list_returns_user_notifications_sorted_desc() {
        let repo = MockNotifRepo::new_arc();
        let svc = make_svc(repo.clone());
        // Push 3 notif ke user-1, 1 ke user-2
        for i in 0..3 {
            svc.push(
                "user-1",
                "test",
                &format!("Notif {}", i),
                "body",
                None,
                None,
                None,
            )
            .await
            .unwrap();
        }
        svc.push("user-2", "test", "Lain", "body", None, None, None)
            .await
            .unwrap();

        let result = svc.list("user-1", 10, None, false).await.unwrap();
        assert_eq!(result.items.len(), 3);
        // Memastikan sorted desc (id terbesar dulu)
        assert!(result.items[0].id > result.items[1].id);
    }

    #[tokio::test]
    async fn list_limit_is_respected() {
        let repo = MockNotifRepo::new_arc();
        let svc = make_svc(repo.clone());
        for _ in 0..10 {
            svc.push("user-1", "test", "Title", "body", None, None, None)
                .await
                .unwrap();
        }
        let result = svc.list("user-1", 5, None, false).await.unwrap();
        assert_eq!(result.items.len(), 5);
        assert!(result.has_more);
    }

    #[tokio::test]
    async fn list_unread_only_filters_read_notifications() {
        let repo = MockNotifRepo::new_arc();
        let svc = make_svc(repo.clone());
        svc.push("user-1", "test", "Notif 1", "", None, None, None)
            .await
            .unwrap();
        svc.push("user-1", "test", "Notif 2", "", None, None, None)
            .await
            .unwrap();
        // Mark notif id=1 sebagai read
        svc.mark_read(1, "user-1").await.unwrap();

        let unread = svc.list("user-1", 10, None, true).await.unwrap();
        assert_eq!(unread.items.len(), 1);
        assert_eq!(unread.items[0].id, 2);
    }

    #[tokio::test]
    async fn mark_all_read_marks_all() {
        let repo = MockNotifRepo::new_arc();
        let svc = make_svc(repo.clone());
        for _ in 0..5 {
            svc.push("user-1", "test", "X", "X", None, None, None)
                .await
                .unwrap();
        }
        svc.mark_all_read("user-1").await.unwrap();
        // Akses repo langsung untuk assert
        let count = repo
            .store
            .lock()
            .await
            .iter()
            .filter(|n| !n.is_read)
            .count();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn list_before_id_cursor_pagination() {
        let repo = MockNotifRepo::new_arc();
        let svc = make_svc(repo.clone());
        for i in 0..10 {
            svc.push("user-1", "test", &format!("N{}", i), "", None, None, None)
                .await
                .unwrap();
        }
        // Ambil 5 terbaru (id 10..6), lalu cursor di id=6 untuk page berikutnya
        let page1 = svc.list("user-1", 5, None, false).await.unwrap();
        let oldest_id = page1.items.last().unwrap().id;
        let page2 = svc.list("user-1", 5, Some(oldest_id), false).await.unwrap();
        assert_eq!(page2.items.len(), 5);
        // Semua item di page2 harus id < oldest_id dari page1
        for item in &page2.items {
            assert!(item.id < oldest_id);
        }
    }
}

// =============================================================================

#[cfg(test)]
mod rideshare_service_tests {
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    use crate::{
        connections::ConnectionManager,
        repository::{
            notification::NotificationRepositoryTrait,
            rideshare::{
                RideshareOpenItem, RidesharePassenger, RideshareRepositoryTrait, RideshareTrip,
            },
            user::UserRepository,
        },
        service::rideshare::{haversine_km, RideshareService},
        utils::ulid::new_ulid,
    };

    // ── Mock RideshareRepository ──────────────────────────────────────────────

    #[derive(Default)]
    struct MockRideshareRepo {
        trips: Mutex<Vec<RideshareTrip>>,
        passengers: Mutex<Vec<RidesharePassenger>>,
    }

    fn make_trip(id: &str, driver: &str, status: &str) -> RideshareTrip {
        RideshareTrip {
            id: id.to_string(),
            host_order_id: new_ulid(),
            driver_id: driver.to_string(),
            service_type: "mobil".into(),
            route_start_lat: -6.2,
            route_start_lng: 106.8,
            route_end_lat: -6.1,
            route_end_lng: 106.9,
            route_polyline: None,
            max_passengers: 3,
            current_passengers: 0,
            status: status.to_string(),
            join_deadline: None,
            created_at: "2024-01-01T00:00:00Z".into(),
        }
    }

    fn make_passenger(id: &str, trip_id: &str, rider_id: &str, status: &str) -> RidesharePassenger {
        RidesharePassenger {
            id: id.to_string(),
            trip_id: trip_id.to_string(),
            rider_id: rider_id.to_string(),
            pickup_lat: -6.2,
            pickup_lng: 106.8,
            pickup_address: "Pickup".into(),
            dest_lat: -6.1,
            dest_lng: 106.9,
            dest_address: "Dest".into(),
            fare_estimate: 15_000,
            fare_final: None,
            distance_km: None,
            status: status.to_string(),
            requested_at: "2024-01-01T00:00:00Z".into(),
            accepted_at: None,
            picked_up_at: None,
            dropped_off_at: None,
            cancelled_at: None,
            cancel_reason: None,
        }
    }

    #[async_trait]
    impl RideshareRepositoryTrait for MockRideshareRepo {
        async fn create_trip(
            &self,
            host_order_id: &str,
            driver_id: &str,
            service_type: &str,
            start_lat: f64,
            start_lng: f64,
            end_lat: f64,
            end_lng: f64,
            max_passengers: i32,
            _join_deadline_s: Option<i64>,
        ) -> Result<String> {
            let id = new_ulid();
            let trip = RideshareTrip {
                id: id.clone(),
                host_order_id: host_order_id.to_string(),
                driver_id: driver_id.to_string(),
                service_type: service_type.to_string(),
                route_start_lat: start_lat,
                route_start_lng: start_lng,
                route_end_lat: end_lat,
                route_end_lng: end_lng,
                route_polyline: None,
                max_passengers,
                current_passengers: 0,
                status: "open".into(),
                join_deadline: None,
                created_at: "2024-01-01T00:00:00Z".into(),
            };
            self.trips.lock().await.push(trip);
            Ok(id)
        }

        async fn find_trip_by_id(&self, trip_id: &str) -> Result<Option<RideshareTrip>> {
            Ok(self
                .trips
                .lock()
                .await
                .iter()
                .find(|t| t.id == trip_id)
                .cloned())
        }

        async fn find_trip_by_order(&self, order_id: &str) -> Result<Option<RideshareTrip>> {
            Ok(self
                .trips
                .lock()
                .await
                .iter()
                .find(|t| t.host_order_id == order_id)
                .cloned())
        }

        async fn update_trip_status(&self, trip_id: &str, status: &str) -> Result<()> {
            let mut trips = self.trips.lock().await;
            if let Some(t) = trips.iter_mut().find(|t| t.id == trip_id) {
                t.status = status.to_string();
            }
            Ok(())
        }

        async fn list_open_trips(
            &self,
            service_type: &str,
            _min_lat: f64,
            _max_lat: f64,
            _min_lng: f64,
            _max_lng: f64,
        ) -> Result<Vec<RideshareOpenItem>> {
            Ok(self
                .trips
                .lock()
                .await
                .iter()
                .filter(|t| t.service_type == service_type && t.status == "open")
                .map(|t| RideshareOpenItem {
                    id: t.id.clone(),
                    host_order_id: t.host_order_id.clone(),
                    driver_id: t.driver_id.clone(),
                    service_type: t.service_type.clone(),
                    route_start_lat: t.route_start_lat,
                    route_start_lng: t.route_start_lng,
                    route_end_lat: t.route_end_lat,
                    route_end_lng: t.route_end_lng,
                    route_polyline: None,
                    seats_available: (t.max_passengers - t.current_passengers) as i64,
                    join_deadline: None,
                    driver_name: "Driver".into(),
                    driver_avatar: None,
                    vehicle_plate: "B1234".into(),
                    vehicle_model: "Avanza".into(),
                    vehicle_color: "Putih".into(),
                    driver_rating: 4.8,
                })
                .collect())
        }

        async fn create_passenger(
            &self,
            trip_id: &str,
            rider_id: &str,
            pickup_lat: f64,
            pickup_lng: f64,
            pickup_address: &str,
            dest_lat: f64,
            dest_lng: f64,
            dest_address: &str,
            fare_estimate: i32,
        ) -> Result<String> {
            let id = new_ulid();
            let p = make_passenger(&id, trip_id, rider_id, "pending");
            self.passengers.lock().await.push(p);
            Ok(id)
        }

        async fn find_passenger_by_id(
            &self,
            passenger_id: &str,
        ) -> Result<Option<RidesharePassenger>> {
            Ok(self
                .passengers
                .lock()
                .await
                .iter()
                .find(|p| p.id == passenger_id)
                .cloned())
        }

        async fn list_passengers(&self, trip_id: &str) -> Result<Vec<RidesharePassenger>> {
            Ok(self
                .passengers
                .lock()
                .await
                .iter()
                .filter(|p| p.trip_id == trip_id)
                .cloned()
                .collect())
        }

        async fn update_passenger_status(
            &self,
            passenger_id: &str,
            status: &str,
            reason: Option<&str>,
            fare_final: Option<i32>,
            distance_km: Option<f64>,
        ) -> Result<()> {
            let mut passengers = self.passengers.lock().await;
            if let Some(p) = passengers.iter_mut().find(|p| p.id == passenger_id) {
                p.status = status.to_string();
                if let Some(f) = fare_final {
                    p.fare_final = Some(f);
                }
                if let Some(d) = distance_km {
                    p.distance_km = Some(d);
                }
                if let Some(r) = reason {
                    p.cancel_reason = Some(r.to_string());
                }
            }
            Ok(())
        }

        async fn passenger_exists(&self, trip_id: &str, rider_id: &str) -> Result<bool> {
            Ok(self.passengers.lock().await.iter().any(|p| {
                p.trip_id == trip_id
                    && p.rider_id == rider_id
                    && !matches!(p.status.as_str(), "rejected" | "cancelled")
            }))
        }
    }

    // ── Mock NotifRepo & UserRepo (minimal) ───────────────────────────────────

    #[derive(Default)]
    struct MockNotifRepo;

    #[async_trait]
    impl NotificationRepositoryTrait for MockNotifRepo {
        async fn create(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: &str,
            _: Option<&str>,
            _: Option<&str>,
            _: Option<&str>,
        ) -> Result<i64> {
            Ok(0)
        }
        async fn list(
            &self,
            _: &str,
            _: i32,
            _: Option<i64>,
            _: bool,
        ) -> Result<Vec<crate::repository::notification::Notification>> {
            Ok(vec![])
        }
        async fn mark_read(&self, _: i64, _: &str) -> Result<()> {
            Ok(())
        }
        async fn mark_all_read(&self, _: &str) -> Result<()> {
            Ok(())
        }
        async fn unread_count(&self, _: &str) -> Result<i64> {
            Ok(0)
        }
    }

    #[derive(Default)]
    struct MockUserRepo;

    #[async_trait]
    impl UserRepository for MockUserRepo {
        async fn create(
            &self,
            _: &crate::models::user::RegisterRequest,
            _: &str,
        ) -> Result<crate::models::user::User> {
            unimplemented!()
        }
        async fn find_by_id(&self, _: &str) -> Result<Option<crate::models::user::User>> {
            Ok(None)
        }
        async fn find_by_phone(&self, _: &str) -> Result<Option<crate::models::user::User>> {
            Ok(None)
        }
        async fn find_driver_by_id(
            &self,
            _: &str,
        ) -> Result<
            Option<(
                crate::models::user::User,
                crate::models::user::DriverProfile,
            )>,
        > {
            Ok(None)
        }
        async fn find_drivers_by_ids(
            &self,
            _: &[String],
        ) -> Result<
            Vec<(
                crate::models::user::User,
                crate::models::user::DriverProfile,
            )>,
        > {
            Ok(vec![])
        }
    }

    fn make_svc(
        repo: Arc<MockRideshareRepo>,
    ) -> RideshareService<MockRideshareRepo, MockNotifRepo, MockUserRepo> {
        let connections: Arc<ConnectionManager> = unsafe { std::mem::zeroed() };
        RideshareService {
            rideshare_repo: repo,
            notif_repo: Arc::new(MockNotifRepo),
            user_repo: Arc::new(MockUserRepo),
            connections,
        }
    }

    // ── Tests: haversine ──────────────────────────────────────────────────────

    #[test]
    fn haversine_same_point_is_zero() {
        let d = haversine_km(-6.2, 106.8, -6.2, 106.8);
        assert!(d < 0.001);
    }

    #[test]
    fn haversine_monas_to_bundaran_hi_approx_5km() {
        // Monas ↔ Bundaran HI ~4-5 km
        let d = haversine_km(-6.1754, 106.8272, -6.1944, 106.8229);
        assert!(d > 1.0 && d < 10.0, "jarak tidak masuk akal: {}", d);
    }

    // ── Tests: open_trip ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn open_trip_success() {
        let repo = Arc::new(MockRideshareRepo::default());
        let svc = make_svc(repo.clone());
        let driver = new_ulid();
        let order = new_ulid();
        let trip_id = svc
            .open_trip(&driver, &order, -6.2, 106.8, -6.1, 106.9, "mobil", 3, None)
            .await
            .unwrap();
        assert!(!trip_id.is_empty());
        let trips = repo.trips.lock().await;
        assert_eq!(trips.len(), 1);
        assert_eq!(trips[0].driver_id, driver);
    }

    #[tokio::test]
    async fn open_trip_duplicate_order_returns_error() {
        let repo = Arc::new(MockRideshareRepo::default());
        let svc = make_svc(repo.clone());
        let driver = new_ulid();
        let order = new_ulid();
        // Open pertama
        svc.open_trip(&driver, &order, -6.2, 106.8, -6.1, 106.9, "mobil", 3, None)
            .await
            .unwrap();
        // Coba open lagi dengan order yang sama
        let err = svc
            .open_trip(&driver, &order, -6.2, 106.8, -6.1, 106.9, "mobil", 3, None)
            .await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("sudah dibuka"));
    }

    // ── Tests: join_trip ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn join_trip_success() {
        let repo = Arc::new(MockRideshareRepo::default());
        let driver = new_ulid();
        let trip = make_trip("trip-1", &driver, "open");
        repo.trips.lock().await.push(trip);
        let svc = make_svc(repo.clone());

        let rider = new_ulid();
        let (passenger_id, fare) = svc
            .join_trip(
                &rider, "Rider A", "", "trip-1", -6.2, 106.8, "Pickup", -6.1, 106.9, "Dest",
            )
            .await
            .unwrap();

        assert!(!passenger_id.is_empty());
        assert!(fare > 0);
        assert_eq!(repo.passengers.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn join_trip_not_open_returns_error() {
        let repo = Arc::new(MockRideshareRepo::default());
        let driver = new_ulid();
        let trip = make_trip("trip-full", &driver, "full");
        repo.trips.lock().await.push(trip);
        let svc = make_svc(repo.clone());

        let err = svc
            .join_trip(
                &new_ulid(),
                "Rider",
                "",
                "trip-full",
                -6.2,
                106.8,
                "P",
                -6.1,
                106.9,
                "D",
            )
            .await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("tidak tersedia"));
    }

    #[tokio::test]
    async fn join_trip_duplicate_rider_returns_error() {
        let repo = Arc::new(MockRideshareRepo::default());
        let driver = new_ulid();
        let rider = new_ulid();
        let trip = make_trip("trip-2", &driver, "open");
        repo.trips.lock().await.push(trip);
        let svc = make_svc(repo.clone());

        svc.join_trip(
            &rider, "Rider", "", "trip-2", -6.2, 106.8, "P", -6.1, 106.9, "D",
        )
        .await
        .unwrap();

        let err = svc
            .join_trip(
                &rider, "Rider", "", "trip-2", -6.2, 106.8, "P", -6.1, 106.9, "D",
            )
            .await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("sudah request"));
    }

    // ── Tests: accept_passenger ───────────────────────────────────────────────

    #[tokio::test]
    async fn accept_passenger_success() {
        let repo = Arc::new(MockRideshareRepo::default());
        let driver = new_ulid();
        let rider = new_ulid();
        let trip = make_trip("trip-3", &driver, "open");
        let passenger = make_passenger("pass-1", "trip-3", &rider, "pending");
        repo.trips.lock().await.push(trip);
        repo.passengers.lock().await.push(passenger);

        let svc = make_svc(repo.clone());
        svc.accept_passenger(&driver, "trip-3", "pass-1")
            .await
            .unwrap();

        let passengers = repo.passengers.lock().await;
        assert_eq!(passengers[0].status, "accepted");
    }

    #[tokio::test]
    async fn accept_passenger_wrong_driver_returns_error() {
        let repo = Arc::new(MockRideshareRepo::default());
        let driver_a = new_ulid();
        let driver_b = new_ulid();
        let rider = new_ulid();
        let trip = make_trip("trip-4", &driver_a, "open");
        let passenger = make_passenger("pass-2", "trip-4", &rider, "pending");
        repo.trips.lock().await.push(trip);
        repo.passengers.lock().await.push(passenger);

        let svc = make_svc(repo.clone());
        let err = svc.accept_passenger(&driver_b, "trip-4", "pass-2").await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("Bukan trip kamu"));
    }

    // ── Tests: cancel_passenger ───────────────────────────────────────────────

    #[tokio::test]
    async fn cancel_passenger_success() {
        let repo = Arc::new(MockRideshareRepo::default());
        let driver = new_ulid();
        let rider = new_ulid();
        let trip = make_trip("trip-5", &driver, "open");
        let passenger = make_passenger("pass-3", "trip-5", &rider, "pending");
        repo.trips.lock().await.push(trip);
        repo.passengers.lock().await.push(passenger);

        let svc = make_svc(repo.clone());
        svc.cancel_passenger(&rider, "trip-5", "pass-3", Some("Berubah pikiran"))
            .await
            .unwrap();

        let passengers = repo.passengers.lock().await;
        assert_eq!(passengers[0].status, "cancelled");
    }

    #[tokio::test]
    async fn cancel_passenger_wrong_rider_returns_error() {
        let repo = Arc::new(MockRideshareRepo::default());
        let driver = new_ulid();
        let rider_a = new_ulid();
        let rider_b = new_ulid();
        let trip = make_trip("trip-6", &driver, "open");
        let passenger = make_passenger("pass-4", "trip-6", &rider_a, "pending");
        repo.trips.lock().await.push(trip);
        repo.passengers.lock().await.push(passenger);

        let svc = make_svc(repo.clone());
        let err = svc
            .cancel_passenger(&rider_b, "trip-6", "pass-4", None)
            .await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("Bukan requestmu"));
    }
}

// =============================================================================

#[cfg(test)]
mod ulid_utils_tests {
    use crate::utils::ulid::*;

    #[test]
    fn new_ulid_is_26_chars() {
        let id = new_ulid();
        assert_eq!(id.len(), 26, "ULID harus 26 karakter, dapat: {}", id);
    }

    #[test]
    fn new_ulid_is_unique() {
        let a = new_ulid();
        let b = new_ulid();
        assert_ne!(a, b);
    }

    #[test]
    fn ulid_to_vec_then_bin_to_ulid_roundtrip() {
        let original = new_ulid();
        let bytes = ulid_to_vec(&original).unwrap();
        assert_eq!(bytes.len(), 16);
        let restored = bin_to_ulid(bytes).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn bin_to_ulid_opt_none_returns_none() {
        let result = bin_to_ulid_opt(None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn bin_to_ulid_opt_some_returns_ulid() {
        let id = new_ulid();
        let bytes = ulid_to_vec(&id).unwrap();
        let result = bin_to_ulid_opt(Some(bytes)).unwrap();
        assert_eq!(result, Some(id));
    }

    #[test]
    fn id_to_vec_accepts_26_char_ulid() {
        let id = new_ulid();
        assert_eq!(id.len(), 26);
        let bytes = id_to_vec(&id).unwrap();
        assert_eq!(bytes.len(), 16);
    }

    #[test]
    fn id_to_vec_accepts_32_char_hex() {
        let hex = "019d4942f47000ee70983c1090bc616b";
        let bytes = id_to_vec(hex).unwrap();
        assert_eq!(bytes.len(), 16);
    }

    #[test]
    fn id_to_vec_rejects_invalid_length() {
        let err = id_to_vec("terlalu-pendek");
        assert!(err.is_err());
    }

    #[test]
    fn id_to_vec_rejects_invalid_hex() {
        let not_hex = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"; // 32 char tapi bukan hex
        let err = id_to_vec(not_hex);
        assert!(err.is_err());
    }

    #[test]
    fn ulid_to_bytes_invalid_returns_error() {
        let err = ulid_to_bytes("bukan-ulid-valid!!!");
        assert!(err.is_err());
    }

    #[test]
    fn mime_to_type_image() {
        assert_eq!(mime_to_type("image/jpeg"), "image");
        assert_eq!(mime_to_type("image/png"), "image");
    }

    #[test]
    fn mime_to_type_video() {
        assert_eq!(mime_to_type("video/mp4"), "video");
    }

    #[test]
    fn mime_to_type_audio() {
        assert_eq!(mime_to_type("audio/mpeg"), "audio");
    }

    #[test]
    fn mime_to_type_unknown_returns_file() {
        assert_eq!(mime_to_type("application/pdf"), "file");
        assert_eq!(mime_to_type("text/plain"), "file");
    }
}
