using Microsoft.IdentityModel.Tokens;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;

namespace Dotmim.Sync.Tests.Core
{
    public class AuthToken
    {

        public static string Issuer => "Dotmim.Sync.Bearer";
        public static string Audience => "Dotmim.Sync.Bearer";

        public static string SecurityKey => "SOME_RANDOM_KEY_DO_NOT_SHARE";

        /// <summary>
        /// Generate a token
        /// </summary>
        internal static string GenerateJwtToken(string email, string userid)
        {
            var claims = new List<Claim>
            {
                new Claim(JwtRegisteredClaimNames.Sub, "DMS"),
                new Claim(JwtRegisteredClaimNames.Email, email),
                new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString()),
                new Claim(ClaimTypes.NameIdentifier, userid)
            };

            var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(SecurityKey));
            var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);

            var expires = DateTime.Now.AddDays(Convert.ToDouble(10));

            var token = new JwtSecurityToken(
                AuthToken.Issuer,
                AuthToken.Audience,
                claims,
                expires: expires,
                signingCredentials: creds
            );

            return new JwtSecurityTokenHandler().WriteToken(token);
        }
    }
}
