defmodule Sequin.AccountsCloudflareAccessTest do
  use Sequin.DataCase, async: true

  import Ecto.Query

  alias Sequin.Accounts
  alias Sequin.Accounts.User
  alias Sequin.Repo

  describe "find_or_create_cloudflare_access_user/1" do
    test "provisions a new user with their own account on first sign-in" do
      claims = %{"email" => "New.User@Triptech.com", "sub" => "cf-sub-1", "name" => "New User"}

      assert {:ok, %User{} = user} = Accounts.find_or_create_cloudflare_access_user(claims)

      # Email is downcased, provider + id recorded from the JWT.
      assert user.email == "new.user@triptech.com"
      assert user.name == "New User"
      assert user.auth_provider == :cloudflare_access
      assert user.auth_provider_id == "cf-sub-1"
      assert is_nil(user.hashed_password)

      # Gets their own account (new-account-per-user model).
      assert %{} = account = User.current_account(user)
      assert account.id
    end

    test "is idempotent — returns the existing user without duplicating" do
      claims = %{"email" => "dup@triptech.com", "sub" => "cf-sub-2", "name" => "Dup"}

      assert {:ok, user1} = Accounts.find_or_create_cloudflare_access_user(claims)
      assert {:ok, user2} = Accounts.find_or_create_cloudflare_access_user(claims)

      assert user1.id == user2.id
      assert Repo.aggregate(from(u in User, where: u.email == "dup@triptech.com"), :count) == 1
    end

    test "matches an existing user case-insensitively via the downcased email" do
      {:ok, user} = Accounts.find_or_create_cloudflare_access_user(%{"email" => "casing@triptech.com", "sub" => "s"})

      assert {:ok, again} =
               Accounts.find_or_create_cloudflare_access_user(%{"email" => "CASING@Triptech.com", "sub" => "s"})

      assert again.id == user.id
    end

    test "falls back to email when the sub claim is missing" do
      assert {:ok, user} = Accounts.find_or_create_cloudflare_access_user(%{"email" => "nosub@triptech.com"})
      assert user.auth_provider_id == "nosub@triptech.com"
    end

    test "returns an error when the email claim is missing" do
      assert {:error, _} = Accounts.find_or_create_cloudflare_access_user(%{"sub" => "cf-sub-3"})
    end
  end
end
