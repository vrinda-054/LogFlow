"""Focused tests for LogFlow authentication validation and token/session behavior."""

import os
import unittest
from unittest.mock import patch

from fastapi import HTTPException, Response
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError

from processing.auth.dependencies import get_current_user
from processing.auth.router import login, logout, signup
from processing.auth.schemas import LoginRequest, SignupRequest
from processing.auth.security import hash_password, verify_password


class AuthValidationTests(unittest.TestCase):
    def test_signup_normalizes_email_and_name(self) -> None:
        request = SignupRequest(name="  Jane Doe ", email=" JANE@EXAMPLE.COM ", password="secure123")
        self.assertEqual(request.name, "Jane Doe")
        self.assertEqual(request.email, "jane@example.com")

    def test_signup_rejects_invalid_email(self) -> None:
        with self.assertRaises(ValidationError):
            SignupRequest(name="Jane", email="not-an-email", password="secure123")

    def test_signup_rejects_short_password(self) -> None:
        with self.assertRaises(ValidationError):
            SignupRequest(name="Jane", email="jane@example.com", password="short")

    def test_password_hash_is_not_plaintext_and_verifies(self) -> None:
        password_hash = hash_password("secure123")
        self.assertNotEqual(password_hash, "secure123")
        self.assertTrue(verify_password("secure123", password_hash))
        self.assertFalse(verify_password("wrong-password", password_hash))


class AuthEndpointTests(unittest.TestCase):
    def setUp(self) -> None:
        os.environ["JWT_SECRET_KEY"] = "test-secret-for-auth-tests"

    @patch("processing.auth.router.create_user")
    @patch("processing.auth.router.create_access_token", return_value=("token", 1800))
    def test_signup_returns_user_and_sets_cookie(self, create_token, create_user) -> None:
        create_user.return_value = {"id": 1, "name": "Jane", "email": "jane@example.com", "is_active": True}
        response = Response()
        result = signup(SignupRequest(name="Jane", email="jane@example.com", password="secure123"), response)
        self.assertEqual(result.user.email, "jane@example.com")
        self.assertIn("logflow_session=token", response.headers["set-cookie"])

    @patch("processing.auth.router.create_user", side_effect=IntegrityError("insert", {}, Exception("duplicate")))
    def test_signup_does_not_expose_database_errors(self, _create_user) -> None:
        with self.assertRaises(HTTPException) as error:
            signup(SignupRequest(name="Jane", email="jane@example.com", password="secure123"), Response())
        self.assertEqual(error.exception.status_code, 409)
        self.assertEqual(error.exception.detail, "An account with this email already exists.")

    @patch("processing.auth.router.authenticate_user", return_value=None)
    def test_login_rejects_invalid_credentials_generically(self, _authenticate) -> None:
        with self.assertRaises(HTTPException) as error:
            login(LoginRequest(email="jane@example.com", password="wrongpass"), Response())
        self.assertEqual(error.exception.status_code, 401)
        self.assertEqual(error.exception.detail, "Invalid email or password.")

    @patch("processing.auth.router.create_access_token", return_value=("token", 1800))
    @patch("processing.auth.router.authenticate_user")
    def test_login_returns_user_without_password_hash(self, authenticate, _create_token) -> None:
        authenticate.return_value = {"id": 1, "name": "Jane", "email": "jane@example.com", "password_hash": "hidden", "is_active": True}
        result = login(LoginRequest(email="jane@example.com", password="secure123"), Response())
        self.assertEqual(result.user.name, "Jane")
        self.assertFalse(hasattr(result.user, "password_hash"))

    def test_logout_clears_cookie(self) -> None:
        response = Response()
        logout(response)
        self.assertIn('logflow_session=""', response.headers["set-cookie"])
        self.assertIn("Max-Age=0", response.headers["set-cookie"])

    @patch("processing.auth.dependencies.find_user_by_id", return_value=None)
    @patch("processing.auth.dependencies.decode_access_token", return_value=1)
    def test_current_user_rejects_deleted_user(self, _decode, _find_user) -> None:
        with self.assertRaises(HTTPException) as error:
            import asyncio
            asyncio.run(get_current_user("token"))
        self.assertEqual(error.exception.status_code, 401)


if __name__ == "__main__":
    unittest.main()
